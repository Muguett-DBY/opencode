import { describe, expect } from "bun:test"
import photon from "@silvia-odwyer/photon-node"
import { createHash } from "node:crypto"
import { Deferred, Effect, Ref } from "effect"
import { TestClock } from "effect/testing"
import { Photon } from "@opencode/core/image/photon"
import { it } from "./lib/effect"

const limits = { autoResize: true, maxWidth: 2_000, maxHeight: 2_000, maxBase64Bytes: 5 * 1024 * 1024 }

// 2400×1200 with a translucent right half: it resizes, tries PNG first, then lands on JPEG.
const content = (() => {
  const width = 2400
  const height = 1200
  const pixels = new Uint8Array(width * height * 4)
  for (let y = 0; y < height; y++)
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 4
      pixels[i] = (x * 7 + y * 3) & 255
      pixels[i + 1] = (x ^ y) & 255
      pixels[i + 2] = (x * y) & 255
      pixels[i + 3] = x < 1200 ? 255 : y & 255
    }
  const source = new photon.PhotonImage(pixels, width, height)
  const bytes = source.get_bytes()
  source.free()
  return {
    uri: "file:///fixture.png",
    content: Buffer.from(bytes).toString("base64"),
    encoding: "base64" as const,
    mime: "image/png",
  }
})()

// The output of the in-process resizer before it moved to a worker.
const expected = { mime: "image/jpeg", sha256: "356a08f3cd35a6277f6a066ab12141d26212a93d8ec5f53c66af7b9c5844db95" }
const digest = (result: { mime: string; content: string }) => ({
  mime: result.mime,
  sha256: createHash("sha256").update(result.content).digest("hex"),
})

describe("Image.Photon", () => {
  it.live("resizes in a worker to the same bytes as before", () =>
    Effect.gen(function* () {
      const normalize = yield* Photon.make
      expect(digest(yield* normalize("fixture.png", content, limits))).toEqual(expected)
    }),
  )

  it.live("falls back to resizing in-process when the worker cannot start", () =>
    Effect.gen(function* () {
      const normalize = yield* Photon.makeWith(
        Photon.spawnWorker(new URL("./missing-photon-worker.js", import.meta.url)),
      )
      expect(digest(yield* normalize("fixture.png", content, limits))).toEqual(expected)
      expect(digest(yield* normalize("fixture.png", content, limits))).toEqual(expected)
    }),
  )

  it.effect("terminates the worker once it has been idle", () =>
    Effect.gen(function* () {
      const spawned = yield* Ref.make(0)
      const exited = yield* Deferred.make<void>()
      const normalize = yield* Photon.makeWith(
        Photon.spawnWorker().pipe(
          Effect.tap((worker) =>
            Ref.update(spawned, (count) => count + 1).pipe(
              Effect.andThen(Effect.sync(() => worker.once("exit", () => Deferred.doneUnsafe(exited, Effect.void)))),
            ),
          ),
        ),
      )
      const small = { ...limits, maxWidth: 200, maxHeight: 200 }
      yield* normalize("fixture.png", content, small)
      yield* normalize("fixture.png", content, small)
      expect(yield* Ref.get(spawned)).toBe(1)
      expect(yield* Deferred.isDone(exited)).toBe(false)

      yield* TestClock.adjust("10 seconds")
      yield* Deferred.await(exited)

      yield* normalize("fixture.png", content, small)
      expect(yield* Ref.get(spawned)).toBe(2)
    }),
  )
})
