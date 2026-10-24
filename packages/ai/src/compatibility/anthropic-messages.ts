export * as MessagesCompatibility from "./anthropic-messages.js"

import type { LanguageModelCompatibility } from "../schema/options.js"

export type Input = Pick<LanguageModelCompatibility, "toolVideo">

export const resolve = (input?: Input): Required<Input> => ({
  toolVideo: input?.toolVideo ?? "nested",
})
