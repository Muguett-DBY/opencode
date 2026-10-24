import { LocationServiceMap } from "@opencode/core/location-services"
import { Database } from "@opencode/core/database/database"
import { SessionMediaBlob } from "@opencode/core/session/media-blob"
import { FileNotFoundError, InvalidRequestError } from "@opencode/protocol/errors"
import { Effect } from "effect"
import { type HttpServerRequest, HttpServerResponse } from "effect/http"
import { mediaBlobEnabled } from "../media-uri"

const INLINE_IMAGE_MIMES = new Set(["image/png", "image/jpeg", "image/gif", "image/webp"])
const CACHE_CONTROL = "private, max-age=31536000, immutable"
const SECURITY_HEADERS = {
  "x-content-type-options": "nosniff",
  "content-security-policy": "default-src 'none'; sandbox",
} as const

/** `media.get` in the `server.experimental` group; gated per request on the request location's config. */
export const makeMediaGet = Effect.gen(function* () {
  const { db } = yield* Database.Service
  const locations = yield* LocationServiceMap.Service
  return (hash: string, request: HttpServerRequest.HttpServerRequest) =>
    Effect.gen(function* () {
      if (!(yield* mediaBlobEnabled(locations, request)))
        return yield* new FileNotFoundError({
          path: SessionMediaBlob.routePath(hash),
          message: "Media blobs are disabled (experimental.mediaBlob)",
        })
      if (!SessionMediaBlob.HASH_PATTERN.test(hash))
        return yield* new InvalidRequestError({
          message: `Invalid media hash: ${hash}`,
        })
      const blob = yield* SessionMediaBlob.get(db, hash)
      if (!blob)
        return yield* new FileNotFoundError({
          path: SessionMediaBlob.routePath(hash),
          message: `Media blob not found: ${hash}`,
        })
      const etag = `"${blob.hash}"`
      const inline = INLINE_IMAGE_MIMES.has(blob.mime)
      const contentType = inline ? blob.mime : "application/octet-stream"
      const headers = {
        "cache-control": CACHE_CONTROL,
        etag,
        ...SECURITY_HEADERS,
        ...(inline ? {} : { "content-disposition": "attachment" }),
      }
      if (matchesIfNoneMatch(request.headers["if-none-match"], etag))
        return HttpServerResponse.empty({ status: 304, headers })
      const body = new Uint8Array(blob.data.buffer, blob.data.byteOffset, blob.data.byteLength)
      return HttpServerResponse.uint8Array(body, {
        status: 200,
        contentType,
        headers,
      })
    })
})

function matchesIfNoneMatch(ifNoneMatch: string | undefined, etag: string) {
  if (ifNoneMatch === undefined) return false
  return ifNoneMatch.split(",").some((tag) => {
    const trimmed = tag.trim()
    return trimmed === "*" || stripWeak(trimmed) === stripWeak(etag)
  })
}

function stripWeak(etag: string) {
  return etag.replace(/^w\//i, "")
}
