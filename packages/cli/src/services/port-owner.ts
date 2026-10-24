import { AppProcess } from "@opencode/util/process"
import { Effect } from "effect"
import { ChildProcess } from "effect/process"

export * as PortOwner from "./port-owner"

// Best effort: names the process listening on a TCP port so a failed bind can say who holds it.
// Windows has no cheap equivalent of lsof, so it keeps the generic message.
export const describe = Effect.fnUntraced(function* (port: number) {
  if (process.platform === "win32") return undefined
  const appProcess = yield* AppProcess.Service
  const run = (command: string, args: ReadonlyArray<string>) =>
    appProcess
      .run(ChildProcess.make(command, args, { stdin: "ignore" }), { timeout: "1 second", maxOutputBytes: 4096 })
      .pipe(
        Effect.map((result) => (result.exitCode === 0 ? result.stdout.toString("utf8").trim() : "")),
        Effect.orElseSucceed(() => ""),
      )
  const pid = (yield* run("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"])).split("\n")[0]
  if (pid === undefined || !/^\d+$/.test(pid)) return undefined
  const command = (yield* run("ps", ["-o", "command=", "-p", pid])).replace(/\s+/g, " ")
  if (!command) return `PID ${pid}`
  return `PID ${pid} (${command.length > 200 ? command.slice(0, 199) + "…" : command})`
})
