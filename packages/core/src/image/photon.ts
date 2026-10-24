export * as Photon from "./photon.js"

import { Effect, RcRef, Schema } from "effect"
import type { Worker } from "node:worker_threads"
import type { FileSystem } from "../filesystem.js"
import { DecodeError, ResizerUnavailableError, SizeError, type Limits } from "../image.js"
import { PhotonNormalize } from "./photon-normalize.js"
import type { Reply, Request } from "./photon-worker.js"

// The worker holds photon's wasm heap (hundreds of MB after a large image);
// terminating it when idle returns that memory to the OS.
const WORKER_IDLE_TIME_TO_LIVE = "10 seconds"

class WorkerUnavailableError extends Schema.TaggedError<WorkerUnavailableError>()(
  "Image.Photon.WorkerUnavailableError",
  {},
) {}

class WorkerCrashedError extends Schema.TaggedError<WorkerCrashedError>()("Image.Photon.WorkerCrashedError", {
  message: Schema.String,
}) {}

type Thread = {
  readonly run: (
    content: PhotonNormalize.Content,
    limits: Readonly<Limits>,
  ) => Effect.Effect<PhotonNormalize.Result, WorkerCrashedError>
}

// Resolves once the worker has loaded photon. Anything else (no worker_threads,
// a bundle without the worker entry, photon failing to load) means the worker
// is unavailable.
export const spawnWorker = (url?: URL) =>
  Effect.tryPromise({
    try: async () => {
      const { Worker } = await import("node:worker_threads")
      const worker = new Worker(url ?? new URL("./photon-worker.js", import.meta.url))
      await new Promise<void>((resolve, reject) => {
        const fail = (cause: unknown) => {
          worker.off("message", ready)
          reject(cause)
        }
        const ready = () => {
          worker.off("error", fail)
          worker.off("exit", fail)
          resolve()
        }
        worker.once("message", ready)
        worker.once("error", fail)
        worker.once("exit", fail)
      })
      return worker
    },
    catch: () => new WorkerUnavailableError(),
  })

function thread(worker: Worker): Thread {
  const pending = new Map<number, (reply: Exclude<Reply, { _tag: "Ready" }>) => void>()
  let next = 0
  let crashed: string | undefined
  const crash = (message: string) => {
    crashed ??= message
    for (const [id, resume] of pending) resume({ _tag: "Defect", id, message })
    pending.clear()
  }
  // Only an in-flight request keeps the process alive.
  const settle = () => (pending.size === 0 ? worker.unref() : worker.ref())
  worker.on("message", (reply: Reply) => {
    if (reply._tag === "Ready") return
    pending.get(reply.id)?.(reply)
    pending.delete(reply.id)
    settle()
  })
  worker.on("error", (error) => crash(error.message))
  worker.on("exit", (code) => crash(`photon worker exited with code ${code}`))
  settle()
  return {
    run: (content, limits) =>
      Effect.callback<PhotonNormalize.Result, WorkerCrashedError>((resume) => {
        if (crashed !== undefined) return resume(Effect.fail(new WorkerCrashedError({ message: crashed })))
        const id = next++
        pending.set(id, (reply) =>
          resume(
            reply._tag === "Done"
              ? Effect.succeed(reply.result)
              : Effect.fail(new WorkerCrashedError({ message: reply.message })),
          ),
        )
        settle()
        worker.postMessage({ id, content, limits } satisfies Request)
        return Effect.sync(() => {
          pending.delete(id)
          settle()
        })
      }),
  }
}

export const makeWith = Effect.fnUntraced(function* (spawn: Effect.Effect<Worker, WorkerUnavailableError>) {
  const loadPhoton = yield* Effect.cached(
    Effect.tryPromise({
      try: () => PhotonNormalize.load(),
      catch: () => new ResizerUnavailableError(),
    }),
  )
  const worker = yield* RcRef.make({
    acquire: Effect.acquireRelease(spawn, (worker) => Effect.promise(() => worker.terminate())).pipe(
      Effect.map(thread),
    ),
    idleTimeToLive: WORKER_IDLE_TIME_TO_LIVE,
  })
  let useWorker = true

  const inWorker = (content: PhotonNormalize.Content, limits: Readonly<Limits>) =>
    RcRef.get(worker).pipe(
      Effect.flatMap((thread) => thread.run(content, limits)),
      Effect.scoped,
      Effect.catchTag("Image.Photon.WorkerCrashedError", (error) =>
        // A crashed or panicked wasm instance is unusable; the next call spawns a fresh worker.
        RcRef.invalidate(worker).pipe(Effect.andThen(Effect.die(new Error(error.message)))),
      ),
    )
  const inProcess = (content: PhotonNormalize.Content, limits: Readonly<Limits>) =>
    loadPhoton.pipe(Effect.map((photon) => PhotonNormalize.normalize(photon, content, limits)))

  return Effect.fn("Image.Photon.normalize")(function* (
    resource: string,
    content: FileSystem.Content & { readonly encoding: "base64" },
    limits: Readonly<Limits>,
  ) {
    // Fail typed before touching URLs, workers or module loading: workerd has
    // no photon artifact, and a throw outside the typed path (its undefined
    // import.meta.url was one) turns any image-bearing prompt into a 500.
    if (!PhotonNormalize.available) return yield* new ResizerUnavailableError()
    const result = useWorker
      ? yield* inWorker(content, limits).pipe(
          Effect.catchTag("Image.Photon.WorkerUnavailableError", () => {
            useWorker = false
            return inProcess(content, limits)
          }),
        )
      : yield* inProcess(content, limits)
    switch (result._tag) {
      case "Content":
        return result.content
      case "DecodeError":
        return yield* new DecodeError({ resource })
      case "SizeError": {
        const { _tag, ...size } = result
        return yield* new SizeError({ resource, ...size })
      }
    }
  })
})

export const make = makeWith(spawnWorker())
