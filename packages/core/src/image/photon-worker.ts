import { parentPort } from "node:worker_threads"
import type { Limits } from "../image.js"
import { PhotonNormalize } from "./photon-normalize.js"

export type Request = {
  readonly id: number
  readonly content: PhotonNormalize.Content
  readonly limits: Readonly<Limits>
}
export type Reply =
  | { readonly _tag: "Ready" }
  | { readonly _tag: "Done"; readonly id: number; readonly result: PhotonNormalize.Result }
  | { readonly _tag: "Defect"; readonly id: number; readonly message: string }

const port = parentPort
if (port) {
  // A photon load failure throws out of the worker before Ready, so the server
  // falls back to resizing in-process.
  void PhotonNormalize.load().then((photon) => {
    port.on("message", (request: Request) => {
      const reply: Reply = (() => {
        try {
          return {
            _tag: "Done",
            id: request.id,
            result: PhotonNormalize.normalize(photon, request.content, request.limits),
          }
        } catch (cause) {
          return { _tag: "Defect", id: request.id, message: cause instanceof Error ? cause.message : String(cause) }
        }
      })()
      port.postMessage(reply)
    })
    port.postMessage({ _tag: "Ready" } satisfies Reply)
  })
}
