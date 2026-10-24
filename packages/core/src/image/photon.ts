import photonWasm from "#photon-wasm"
import { Effect, Result } from "effect"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { FileSystem } from "../filesystem.js"
import { DEFAULT_LIMITS, DecodeError, ResizerUnavailableError, SizeError, type Limits } from "../image.js"

const PNG_PASSTHROUGH_MAX_BASE64_BYTES = 384 * 1024
const SMALL_PNG_MAX_PIXELS = 256 * 256
const JPEG_QUALITIES = [80, 70, 55, 40]

type PhotonModule = typeof import("@silvia-odwyer/photon-node")
let photonModule: PhotonModule | undefined

// Base64 uses four bytes per three input bytes, including padding.
const base64Length = (bytes: Uint8Array) => Math.ceil(bytes.length / 3) * 4

// Samples an evenly spaced grid of up to 64×64 pixels.
function hasTransparency(pixels: Uint8Array, width: number, height: number) {
  const columns = Math.min(64, width)
  const rows = Math.min(64, height)
  let transparent = 0
  for (let row = 0; row < rows; row++) {
    const y = Math.floor(((row + 0.5) * height) / rows)
    for (let column = 0; column < columns; column++) {
      const x = Math.floor(((column + 0.5) * width) / columns)
      if (pixels[(y * width + x) * 4 + 3]! < 250) transparent++
    }
  }
  return transparent / (columns * rows) > 0.05
}

export const loadPhoton = Effect.suspend(() =>
  // A runtime without a photon wasm artifact (#photon-wasm resolves to the
  // empty string on workerd) has no resizer, by declaration: fail typed
  // before touching URLs or module loading. The path resolution and import
  // for runtimes that DO have an artifact stay inside the guard too — a
  // throw outside it (workerd's undefined import.meta.url was one) is a
  // defect that escapes the ResizerUnavailableError handling and turns any
  // image-bearing prompt into a 500 instead of degrading to passthrough.
  photonWasm === ""
    ? Effect.fail(new ResizerUnavailableError())
    : photonModule
      ? Effect.succeed(photonModule)
      : Effect.tryPromise({
          try: async () => {
            ;(globalThis as typeof globalThis & { __OPENCODE_PHOTON_WASM_PATH?: string }).__OPENCODE_PHOTON_WASM_PATH =
              path.isAbsolute(photonWasm) ? photonWasm : fileURLToPath(new URL(photonWasm, import.meta.url))
            photonModule = await import("@silvia-odwyer/photon-node")
            return photonModule
          },
          catch: () => new ResizerUnavailableError(),
        }),
)

export function normalizeBytesSync(
  photon: PhotonModule,
  resource: string,
  input: Uint8Array,
  mime: string,
  limits: Readonly<Limits>,
  base64Bytes = base64Length(input),
): Result.Result<
  {
    readonly bytes: Uint8Array
    readonly mime: string
    readonly width: number
    readonly height: number
    readonly changed: boolean
  },
  DecodeError | SizeError
> {
  let decoded: InstanceType<PhotonModule["PhotonImage"]>
  try {
    decoded = photon.PhotonImage.new_from_byteslice(input)
  } catch {
    return Result.fail(new DecodeError({ resource }))
  }
  try {
    const width = decoded.get_width()
    const height = decoded.get_height()
    const fits = width <= limits.maxWidth && height <= limits.maxHeight && base64Bytes <= limits.maxBase64Bytes
    const smallPng = base64Bytes <= PNG_PASSTHROUGH_MAX_BASE64_BYTES || width * height <= SMALL_PNG_MAX_PIXELS
    if (fits && (!limits.autoResize || mime !== "image/png" || smallPng))
      return Result.succeed({ bytes: input, mime, width, height, changed: false })
    if (!limits.autoResize)
      return Result.fail(
        new SizeError({
          resource,
          width,
          height,
          bytes: base64Bytes,
          maxWidth: limits.maxWidth,
          maxHeight: limits.maxHeight,
          maxBytes: limits.maxBase64Bytes,
        }),
      )
    const transparent = mime !== "image/jpeg" && hasTransparency(decoded.get_raw_pixels(), width, height)
    // An image that already fits only changes when JPEG makes it smaller.
    if (fits) {
      if (transparent) return Result.succeed({ bytes: input, mime, width, height, changed: false })
      const jpeg = decoded.get_bytes_jpeg(JPEG_QUALITIES[0]!)
      return base64Length(jpeg) < base64Bytes
        ? Result.succeed({ bytes: jpeg, mime: "image/jpeg", width, height, changed: true })
        : Result.succeed({ bytes: input, mime, width, height, changed: false })
    }
    const scale = Math.min(1, limits.maxWidth / width, limits.maxHeight / height)
    const sizes = Array.from({ length: 32 }).reduce<Array<{ width: number; height: number }>>((acc) => {
      const previous = acc.at(-1) ?? {
        width: Math.max(1, Math.round(width * scale)),
        height: Math.max(1, Math.round(height * scale)),
      }
      const next =
        acc.length === 0
          ? previous
          : {
              width: previous.width === 1 ? 1 : Math.max(1, Math.floor(previous.width * 0.75)),
              height: previous.height === 1 ? 1 : Math.max(1, Math.floor(previous.height * 0.75)),
            }
      return acc.some((item) => item.width === next.width && item.height === next.height) ? acc : [...acc, next]
    }, [])
    for (const size of sizes) {
      const resized =
        size.width === width && size.height === height
          ? decoded
          : photon.resize(decoded, size.width, size.height, photon.SamplingFilter.Lanczos3)
      try {
        const pngEncoder = ["image/png", () => resized.get_bytes()] as const
        const jpegEncoders = JPEG_QUALITIES.map(
          (quality) => ["image/jpeg", () => resized.get_bytes_jpeg(quality)] as const,
        )
        const pngFirst = transparent || size.width * size.height <= SMALL_PNG_MAX_PIXELS
        const encoders: Array<readonly [mime: string, encode: () => Uint8Array]> = pngFirst
          ? [pngEncoder, ...jpegEncoders]
          : [...jpegEncoders, pngEncoder]
        for (const [nextMime, encode] of encoders) {
          const candidate = encode()
          if (base64Length(candidate) <= limits.maxBase64Bytes)
            return Result.succeed({
              bytes: candidate,
              mime: nextMime,
              width: size.width,
              height: size.height,
              changed: true,
            })
        }
      } finally {
        if (resized !== decoded) resized.free()
      }
    }
    return Result.fail(
      new SizeError({
        resource,
        width,
        height,
        bytes: base64Bytes,
        maxWidth: limits.maxWidth,
        maxHeight: limits.maxHeight,
        maxBytes: limits.maxBase64Bytes,
      }),
    )
  } finally {
    decoded.free()
  }
}

export function shrinkSync(
  bytes: Uint8Array,
  mime: string,
  limits: Readonly<Limits> = DEFAULT_LIMITS,
): {
  readonly bytes: Uint8Array
  readonly mime: string
  readonly width?: number
  readonly height?: number
} {
  if (!mime.startsWith("image/") || !photonModule) return { bytes, mime }
  const result = normalizeBytesSync(photonModule, "media", bytes, mime, limits)
  if (Result.isFailure(result)) return { bytes, mime }
  return {
    bytes: result.success.bytes,
    mime: result.success.mime,
    width: result.success.width,
    height: result.success.height,
  }
}

export const make = Effect.gen(function* () {
  return Effect.fn("Image.Photon.normalize")(function* (
    resource: string,
    content: FileSystem.Content & { readonly encoding: "base64" },
    limits: Readonly<Limits>,
  ) {
    const photon = yield* loadPhoton
    const input = Buffer.from(content.content, "base64")
    const bytes = Buffer.byteLength(content.content, "utf-8")
    const normalized = normalizeBytesSync(photon, resource, input, content.mime, limits, bytes)
    if (Result.isFailure(normalized)) return yield* Effect.fail(normalized.failure)
    const result = normalized.success
    if (!result.changed) return content
    return {
      ...content,
      content: Buffer.from(result.bytes).toString("base64"),
      encoding: "base64" as const,
      mime: result.mime,
    }
  })
})
