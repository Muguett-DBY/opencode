#!/usr/bin/env bun
/**
 * Moves inline base64 media from existing `session_message.data` and `session_inbox.payload` rows
 * into `media_blob` (normalizing images to <=2000px outside the write transaction and storing raw
 * bytes), populates `media_ref`, sweeps unreferenced blobs, and optionally compacts the database
 * into a new file. Safe to re-run: already-externalized `media:<hash>` refs are skipped while
 * ensuring their `media_ref` entries exist; legacy `/api/media/<hash>` refs are rewritten to `media:<hash>`.
 *
 * Locking & disk space notes:
 * - Each batch acquires a write lock with `BEGIN IMMEDIATE` and a short `busy_timeout` (1000ms by
 *   default), failing fast if a running OpenCode server or another writer holds a lock on the DB.
 * - `--vacuum-into <out.db>` writes a compacted copy of the entire database and requires free disk
 *   space roughly equal to the current size of `<opencode.db>`.
 *
 * Usage:
 *   bun packages/core/script/migrate-media-blobs.ts <path/to/opencode.db> [--vacuum-into <out.db>]
 */
import { Database } from "bun:sqlite"
import { Effect } from "effect"
import { Image } from "../src/image.js"
import { SessionMediaBlob } from "../src/session/media-blob.js"

export const MAX_BATCH_ROWS = 25
export const MAX_BATCH_BYTES = 50 * 1024 * 1024
export const DEFAULT_BUSY_TIMEOUT_MS = 1_000

type RowData = Record<string, unknown>

function normalizeInlineImages<T extends RowData>(data: T): T {
  if ("files" in data && Array.isArray(data.files)) {
    let changed = false
    const files = data.files.map((file: Record<string, unknown>) => {
      if (typeof file?.data !== "string" || file.data.length === 0 || typeof file.mime !== "string") return file
      if (!file.mime.startsWith("image/")) return file
      const raw = Buffer.from(file.data, "base64")
      const shrunk = Image.shrinkSync(raw, file.mime)
      if (shrunk.bytes === raw && shrunk.mime === file.mime) return file
      changed = true
      return {
        ...file,
        data: Buffer.from(shrunk.bytes).toString("base64"),
        mime: shrunk.mime,
      }
    })
    return (changed ? { ...data, files } : data) as T
  }

  if (!("content" in data) || !Array.isArray(data.content)) return data
  let changed = false
  const content = data.content.map((part: Record<string, unknown>) => {
    const state = part?.state as Record<string, unknown> | undefined
    if (part?.type !== "tool" || (state?.status !== "completed" && state?.status !== "error")) return part
    if (!Array.isArray(state.content)) return part
    let partChanged = false
    const items = state.content.map((item: Record<string, unknown>) => {
      if (item?.type !== "file" || typeof item.uri !== "string" || typeof item.mime !== "string") return item
      if (SessionMediaBlob.hashFromUri(item.uri) !== undefined || !item.mime.startsWith("image/")) return item
      const match = /^data:[^,]*;base64,(.+)$/s.exec(item.uri)
      if (!match) return item
      const raw = Buffer.from(match[1]!, "base64")
      const shrunk = Image.shrinkSync(raw, item.mime)
      if (shrunk.bytes === raw && shrunk.mime === item.mime) return item
      partChanged = true
      return {
        ...item,
        uri: `data:${shrunk.mime};base64,${Buffer.from(shrunk.bytes).toString("base64")}`,
        mime: shrunk.mime,
      }
    })
    if (!partChanged) return part
    changed = true
    return { ...part, state: { ...state, content: items } }
  })
  return (changed ? { ...data, content } : data) as T
}

type PreparedItem = {
  readonly id: string
  readonly sessionID: string
  readonly seq: number
  readonly originalJson: string
  readonly json: string | undefined
  readonly blobs: ReadonlyArray<SessionMediaBlob.Blob>
  readonly hashes: ReadonlyArray<string>
  readonly bytes: number
}

export async function migrateMediaBlobs(
  db: Database,
  options?: {
    readonly vacuumInto?: string
    readonly now?: number
    readonly busyTimeoutMs?: number
    readonly maxBatchRows?: number
    readonly maxBatchBytes?: number
    readonly onBeforeBatchCommit?: (ids: ReadonlyArray<string>) => void
  },
) {
  await Effect.runPromise(Image.preload)

  const busyTimeoutMs = options?.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS
  const maxBatchRows = options?.maxBatchRows ?? MAX_BATCH_ROWS
  const maxBatchBytes = options?.maxBatchBytes ?? MAX_BATCH_BYTES
  const now = options?.now ?? Date.now()

  db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(busyTimeoutMs))};`)

  db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS media_blob (
        hash text PRIMARY KEY,
        mime text NOT NULL,
        bytes integer NOT NULL,
        width integer,
        height integer,
        data blob NOT NULL,
        time_created integer NOT NULL
      );
      CREATE TABLE IF NOT EXISTS media_ref (
        session_id text NOT NULL,
        message_id text NOT NULL,
        hash text NOT NULL,
        seq integer NOT NULL,
        CONSTRAINT media_ref_pk PRIMARY KEY(session_id, message_id, hash),
        CONSTRAINT fk_media_ref_session_id_session_v2_id_fk FOREIGN KEY (session_id) REFERENCES session_v2(id) ON DELETE CASCADE,
        CONSTRAINT fk_media_ref_hash_media_blob_hash_fk FOREIGN KEY (hash) REFERENCES media_blob(hash) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS media_ref_hash_idx ON media_ref (hash);
      CREATE INDEX IF NOT EXISTS media_ref_session_seq_idx ON media_ref (session_id, seq);
    `)
  }).immediate()

  const insertBlob = db.prepare(
    `INSERT OR IGNORE INTO media_blob (hash, mime, bytes, width, height, data, time_created) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
  const upsertRef = db.prepare(
    `INSERT INTO media_ref (session_id, message_id, hash, seq) VALUES (?, ?, ?, ?)
     ON CONFLICT(session_id, message_id, hash) DO UPDATE SET seq = excluded.seq`,
  )

  let candidates = 0
  let migratedRows = 0
  let skippedChanged = 0
  let blobsStored = 0
  let refsRecorded = 0

  const migrateTable = (
    table: "session_message" | "session_inbox",
    column: "data" | "payload",
    seqColumn: "seq" | "enqueued_seq",
  ) => {
    const exists = db
      .query<{ name: string }, [string]>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`)
      .get(table)
    if (!exists) return

    const ids = db
      .query<{ id: string }, []>(
        `SELECT id FROM ${table} WHERE ${column} LIKE '%;base64,%' OR ${column} LIKE '%"files"%' OR ${column} LIKE '%"media:%' OR ${column} LIKE '%/api/media/%'`,
      )
      .all()
    candidates += ids.length

    const selectRow = db.prepare<{ session_id: string; seq: number; json: string }, [string]>(
      `SELECT session_id, ${seqColumn} AS seq, ${column} AS json FROM ${table} WHERE id = ?`,
    )
    const updateRow = db.prepare(`UPDATE ${table} SET ${column} = ? WHERE id = ? AND ${column} = ?`)

    const commitBatch = db.transaction((items: ReadonlyArray<PreparedItem>) => {
      for (const item of items) {
        const current = selectRow.get(item.id)
        if (
          !current ||
          current.json !== item.originalJson ||
          current.seq !== item.seq ||
          current.session_id !== item.sessionID
        ) {
          skippedChanged++
          continue
        }
        if (item.json !== undefined) {
          const updated = updateRow.run(item.json, item.id, item.originalJson)
          if (updated.changes === 0) {
            skippedChanged++
            continue
          }
          migratedRows++
        }
        for (const blob of item.blobs) {
          blobsStored += insertBlob.run(
            blob.hash,
            blob.mime,
            blob.bytes,
            blob.width,
            blob.height,
            blob.data,
            now,
          ).changes
        }
        for (const hash of item.hashes) {
          refsRecorded += upsertRef.run(item.sessionID, item.id, hash, item.seq).changes
        }
      }
    })

    let batch: PreparedItem[] = []
    let batchBytes = 0
    const flush = () => {
      if (batch.length === 0) return
      options?.onBeforeBatchCommit?.(batch.map((item) => item.id))
      commitBatch.immediate(batch)
      batch = []
      batchBytes = 0
    }

    for (const { id } of ids) {
      // Decode, resize, and hash outside the write transaction so the DB lock is only held for fast batch writes.
      const row = selectRow.get(id)
      if (!row) continue
      const parsed = JSON.parse(row.json)
      const normalized = normalizeInlineImages(parsed)
      const split = SessionMediaBlob.split(normalized)
      const changed = split.blobs.length > 0 || split.data !== parsed
      const hashes = [...SessionMediaBlob.collectHashes(split.data)]
      if (!changed && hashes.length === 0) continue

      const itemBytes = split.blobs.reduce((sum, blob) => sum + blob.bytes, 0)
      if (batch.length > 0 && (batch.length >= maxBatchRows || batchBytes + itemBytes > maxBatchBytes)) {
        flush()
      }
      batch.push({
        id,
        sessionID: row.session_id,
        seq: row.seq,
        originalJson: row.json,
        json: changed ? JSON.stringify(split.data) : undefined,
        blobs: split.blobs,
        hashes,
        bytes: itemBytes,
      })
      batchBytes += itemBytes
      if (batch.length >= maxBatchRows || batchBytes >= maxBatchBytes) {
        flush()
      }
    }
    flush()
  }

  migrateTable("session_message", "data", "seq")
  migrateTable("session_inbox", "payload", "enqueued_seq")

  db.transaction(() => {
    db.exec(`DELETE FROM media_blob WHERE NOT EXISTS (SELECT 1 FROM media_ref WHERE media_ref.hash = media_blob.hash);`)
  }).immediate()

  if (options?.vacuumInto) {
    db.exec(`VACUUM INTO '${options.vacuumInto.replace(/'/g, "''")}';`)
  }

  return {
    candidates,
    migratedRows,
    skippedChanged,
    blobsStored,
    refsRecorded,
    vacuumTarget: options?.vacuumInto,
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2)
  const dbPath = args.find((arg) => !arg.startsWith("--"))
  const vacuumIdx = args.indexOf("--vacuum-into")
  const vacuumInto = vacuumIdx !== -1 ? args[vacuumIdx + 1] : undefined

  if (!dbPath) {
    console.error(
      [
        "Usage: bun packages/core/script/migrate-media-blobs.ts <opencode.db> [--vacuum-into <out.db>]",
        "Note: --vacuum-into writes a full compacted copy and requires free disk space roughly equal to <opencode.db>.",
      ].join("\n"),
    )
    process.exit(1)
  }

  const db = new Database(dbPath)
  try {
    const result = await migrateMediaBlobs(db, { vacuumInto })
    console.log(JSON.stringify(result))
  } finally {
    db.close()
  }
}
