export * as SessionMediaBlob from "./media-blob.js"

import { createHash } from "node:crypto"
import { inArray } from "drizzle-orm"
import { Effect } from "effect"
import type { Database } from "../database/database.js"
import { MediaBlobTable, SessionInboxTable, SessionMessageTable } from "./sql.js"

type DatabaseService = Database.Interface["db"]
type MessageRow = typeof SessionMessageTable.$inferSelect
type InboxRow = typeof SessionInboxTable.$inferSelect
type Data = MessageRow["data"] | InboxRow["payload"]

/** A blob's hash is the lowercase hex SHA-256 of its raw (decoded) bytes. */
export const hashBytes = (raw: Uint8Array) => createHash("sha256").update(raw).digest("hex")
export const HASH_PATTERN = /^[0-9a-f]{64}$/

/**
 * Stored rows reference a blob as `media:<hash>` in the existing `uri` field, next to `data:` and `file:` URIs.
 * The ref names no server route; clients that opt in see a fetchable path only in the response projection.
 */
export const REF_SCHEME = "media:"
// Written only by early builds of this feature; still read, never written.
const LEGACY_REF_PREFIX = "/api/media/"

export const ref = (hash: string) => `${REF_SCHEME}${hash}`

type StoredUserFile = {
  readonly data?: string
  readonly uri?: string
  readonly mime: string
  readonly bytes?: number
  readonly width?: number
  readonly height?: number
  readonly source?: unknown
  readonly origin?: unknown
  readonly name?: string
  readonly description?: string
  readonly mention?: unknown
}

type StoredToolFile = {
  readonly type: "file"
  readonly uri: string
  readonly mime: string
  readonly bytes?: number
  readonly width?: number
  readonly height?: number
  readonly name?: string
}

export const hashFromUri = (uri: string | undefined) => {
  const hash = uri?.startsWith(REF_SCHEME)
    ? uri.slice(REF_SCHEME.length)
    : uri?.startsWith(LEGACY_REF_PREFIX)
      ? uri.slice(LEGACY_REF_PREFIX.length)
      : undefined
  return hash !== undefined && HASH_PATTERN.test(hash) ? hash : undefined
}

export function collectHashes(data: Data, target = new Set<string>()) {
  if ("files" in data && data.files) {
    for (const file of data.files as ReadonlyArray<StoredUserFile>) {
      const hash = hashFromUri(file.uri)
      if (hash) target.add(hash)
    }
  }
  if ("content" in data) {
    for (const part of data.content) {
      if (part.type !== "tool" || (part.state.status !== "completed" && part.state.status !== "error")) continue
      for (const item of part.state.content ?? []) {
        if (item.type !== "file") continue
        const hash = hashFromUri(item.uri)
        if (hash) target.add(hash)
      }
    }
  }
  return target
}

function hydrateData<T extends Data>(
  data: T,
  blobs: ReadonlyMap<string, { readonly mime: string; readonly base64: string }>,
): T {
  if ("files" in data && data.files) {
    let changed = false
    const files = (data.files as ReadonlyArray<StoredUserFile>).map((file) => {
      const hash = hashFromUri(file.uri)
      const blob = hash ? blobs.get(hash) : undefined
      if (!blob) return file
      const { uri: _u, bytes: _b, width: _w, height: _h, origin, source, ...rest } = file
      changed = true
      return {
        ...rest,
        data: blob.base64,
        mime: file.mime || blob.mime,
        source: source ?? origin ?? { type: "inline" as const },
      }
    })
    return (changed ? { ...data, files } : data) as T
  }
  if (!("content" in data)) return data
  let changed = false
  const content = data.content.map((part) => {
    if (part.type !== "tool" || (part.state.status !== "completed" && part.state.status !== "error")) return part
    if (!part.state.content) return part
    let partChanged = false
    const items = part.state.content.map((item) => {
      if (item.type !== "file") return item
      const hash = hashFromUri(item.uri)
      const blob = hash ? blobs.get(hash) : undefined
      if (!blob) return item
      const { bytes: _b, width: _w, height: _h, ...rest } = item as StoredToolFile
      partChanged = true
      return {
        ...rest,
        uri: `data:${item.mime || blob.mime};base64,${blob.base64}`,
      }
    })
    if (!partChanged) return part
    changed = true
    return { ...part, state: { ...part.state, content: [items[0]!, ...items.slice(1)] as const } }
  })
  return (changed ? { ...data, content } : data) as T
}

const loadBlobs = Effect.fnUntraced(function* (db: DatabaseService, hashes: ReadonlySet<string>) {
  const all = [...hashes]
  const blobs = new Map<string, { readonly mime: string; readonly base64: string }>()
  for (let offset = 0; offset < all.length; offset += 500) {
    const found = yield* db
      .select({ hash: MediaBlobTable.hash, mime: MediaBlobTable.mime, data: MediaBlobTable.data })
      .from(MediaBlobTable)
      .where(inArray(MediaBlobTable.hash, all.slice(offset, offset + 500)))
      .all()
      .pipe(Effect.orDie)
    for (const blob of found)
      blobs.set(blob.hash, { mime: blob.mime, base64: Buffer.from(blob.data).toString("base64") })
  }
  return blobs
})

/** Restores inline base64 media for any `media:<hash>` refs in `rows`. Rows without refs are returned untouched. */
export const hydrate = Effect.fnUntraced(function* (db: DatabaseService, rows: ReadonlyArray<MessageRow>) {
  const hashes = new Set<string>()
  for (const row of rows) collectHashes(row.data, hashes)
  if (hashes.size === 0) return rows
  const blobs = yield* loadBlobs(db, hashes)
  return rows.map((row) => {
    const data = hydrateData(row.data, blobs)
    return data === row.data ? row : { ...row, data }
  })
})

/** Restores inline base64 media for any `media:<hash>` refs in `session_inbox` rows. */
export const hydrateInbox = Effect.fnUntraced(function* (db: DatabaseService, rows: ReadonlyArray<InboxRow>) {
  const hashes = new Set<string>()
  for (const row of rows) collectHashes(row.payload, hashes)
  if (hashes.size === 0) return rows
  const blobs = yield* loadBlobs(db, hashes)
  return rows.map((row) => {
    const payload = hydrateData(row.payload, blobs)
    return payload === row.payload ? row : { ...row, payload }
  })
})
