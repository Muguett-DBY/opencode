import type { Location } from "@opencode/core/location"
import type { LocationServiceMap } from "@opencode/core/location-services"
import { SessionMediaBlob } from "@opencode/core/session/media-blob"
import { type Context, Effect } from "effect"
import type { HttpServerRequest } from "effect/http"
import { requestRef } from "./location"

type Locations = Context.Service.Shape<typeof LocationServiceMap.Service>

/** `experimental.mediaBlob` from a location's config, read on every call; absent or unresolvable is off. */
export const mediaBlobEnabledAt = (locations: Locations, location: Location.Ref) =>
  SessionMediaBlob.isWriteEnabled.pipe(Effect.provide(locations.get(location)), Effect.orElseSucceed(() => false))

export const mediaBlobEnabled = (locations: Locations, request: HttpServerRequest.HttpServerRequest) =>
  mediaBlobEnabledAt(locations, requestRef(request))

/** Route paths go only to clients that opted in while the flag is on for the request; everyone else gets inline data. */
export const mediaUriFor = (locations: Locations, request: HttpServerRequest.HttpServerRequest) =>
  SessionMediaBlob.hasMediaUriCapability(request.headers) ? mediaBlobEnabled(locations, request) : Effect.succeed(false)
