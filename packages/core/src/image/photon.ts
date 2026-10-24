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

export class WorkerUnavailableError extends Schema.TaggedError<WorkerUnavailableError>()(
  "Image.Photon.WorkerUnavailableError",
  { cause: Schema.Defect() },
) {}

class WorkerCrashedError extends Schema.TaggedError<WorkerCrashedError>()("Image.Photon.WorkerCrashedError", {
  message: Schema.String,
}) {}

type Thread = {
  readonly crashed: () => boolean
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
    catch: (cause) => new WorkerUnavailableError({ cause }),
  })

function thread(worker: Worker): Thread {
  const pending = new Map<number, (result: PhotonNormalize.Result | WorkerCrashedError) => void>()
  let next = 0
  let crashed: WorkerCrashedError | undefined
  // Every in-flight call fails, as a throw from in-process photon would.
  const crash = (message: string) => {
    crashed ??= new WorkerCrashedError({ message })
    for (const resume of pending.values()) resume(crashed)
    pending.clear()
    void worker.terminate()
  }
  // Only an in-flight request keeps the process alive.
  const settle = () => (pending.size === 0 ? worker.unref() : worker.ref())
  worker.on("message", (reply: Reply) => {
    if (reply._tag === "Ready") return
    pending.get(reply.id)?.(reply.result)
    pending.delete(reply.id)
    settle()
  })
  worker.on("messageerror", (error) => crash(error.message))
  worker.on("error", (error) => crash(error.message))
  worker.on("exit", (code) => crash(`photon worker exited with code ${code}`))
  settle()
  return {
    crashed: () => crashed !== undefined,
    run: (content, limits) =>
      Effect.callback<PhotonNormalize.Result, WorkerCrashedError>((resume) => {
        if (crashed !== undefined) return resume(Effect.fail(crashed))
        const id = next++
        pending.set(id, (result) =>
          resume(result instanceof WorkerCrashedError ? Effect.fail(result) : Effect.succeed(result)),
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
    Effect.gen(function* () {
      const current = yield* RcRef.get(worker)
      // A worker that died while idle is replaced rather than failing the call.
      if (!current.crashed()) return yield* current.run(content, limits)
      yield* RcRef.invalidate(worker)
      return yield* (yield* RcRef.get(worker)).run(content, limits)
    }).pipe(
      Effect.scoped,
      Effect.catchTag("Image.Photon.WorkerCrashedError", (error) =>
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
          Effect.catchTag("Image.Photon.WorkerUnavailableError", (error) => {
            if (!useWorker) return inProcess(content, limits)
            useWorker = false
            return Effect.logWarning("photon worker unavailable, resizing images on the main thread", {
              cause: error.cause,
            }).pipe(Effect.andThen(inProcess(content, limits)))
          }),
        )
      : yield* inProcess(content, limits)
    switch (result._tag) {
      case "Unchanged":
        return content
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
