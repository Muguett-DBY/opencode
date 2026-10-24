export * as ResponsesCompatibility from "./open-responses.js"

import type { LanguageModelCompatibility } from "../schema/options.js"

export type Input = Pick<LanguageModelCompatibility, "videoInput" | "toolVideo">

export const resolve = (input?: Input) => ({
  videoInput: input?.videoInput ?? "input_video",
  toolVideo: input?.toolVideo ?? "nested",
})

export type Resolved = ReturnType<typeof resolve>
