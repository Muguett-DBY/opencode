import { expect, test } from "bun:test"
import path from "node:path"

// Windows cannot catch SIGTERM: the process ends at once, so there is no teardown to bound.
test.skipIf(process.platform === "win32")(
  "exits when teardown outlives the deadline and names what is pending",
  async () => {
    const proc = Bun.spawn([process.execPath, path.join(import.meta.dir, "fixture/shutdown-hang.ts"), "200"], {
      cwd: path.join(import.meta.dir, ".."),
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    })
    const pid = Number((await readLine(proc.stdout, "SHUTDOWN_READY "))?.split(" ")[1])
    try {
      expect(pid).toBeGreaterThan(0)
      proc.kill("SIGTERM")
      const stderr = await new Response(proc.stderr).text()
      expect(stderr).toContain("shutdown did not finish within 200ms")
      expect(stderr).toContain("layer test/Hung releasing")
      expect(stderr).toContain(`child ${pid} running: ${process.execPath} -e setInterval`)
      expect(await proc.exited).toBe(1)
    } finally {
      proc.kill("SIGKILL")
      if (running(pid)) process.kill(pid, "SIGKILL")
    }
  },
)

async function readLine(stream: ReadableStream<Uint8Array>, prefix: string) {
  const decoder = new TextDecoder()
  const chunks: string[] = []
  for await (const chunk of stream) {
    chunks.push(decoder.decode(chunk, { stream: true }))
    const line = chunks
      .join("")
      .split("\n")
      .find((line) => line.startsWith(prefix))
    if (line) return line
  }
}

function running(pid: number) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}
