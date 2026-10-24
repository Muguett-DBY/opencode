import { parentPort } from "node:worker_threads"

// Stands in for a photon worker whose wasm panics: it accepts two requests,
// then throws uncaught with both in flight.
const port = parentPort
if (port) {
  let received = 0
  port.on("message", () => {
    if (++received === 2) throw new Error("unreachable executed")
  })
  port.postMessage({ _tag: "Ready" })
}
