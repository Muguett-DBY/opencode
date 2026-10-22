# Restart notices

The captured server stopped the four children. The requested base, `origin/beta` at `9899022996`, already contains newer background-subagent recovery. This change explains interruptions; recovery behavior needs a separate review against the installed build.

## Where the labels came from

- **Interrupted:** `SessionRunner` publishes `session.step.failed` with an `aborted` error. `message-updater.ts` projects that error onto the assistant message. `session-ui/src/timeline/projection.ts` creates a `TurnDivider`; `session-timeline-row.tsx` renders `ui.message.interrupted`. The app uses these shared modules from `app/src/session/timeline/projection.ts` and `message-timeline.tsx`.
- **General cancelled:** the process-local `Job` settles an interrupted child as `cancelled`. `SubagentCompletion.deliver` admits a synthetic message whose metadata contains `source`, `agent`, `childID`, and `state`. Inbox delivery projects that metadata onto the message. The shared timeline notice renderer resolves the agent name and `ui.sessionTimeline.notice.cancelled`.
- The execution lifecycle already records `session.execution.interrupted.reason = shutdown`. Graceful shutdown preserves the execution claim. Startup recovery also records a synthetic `notice: restart`, including after process death without finalizers.

## What changes

- The message projection and the client's live reducer (`client/src/solid/data.ts`) enrich the last aborted assistant with `interruptionReason: shutdown` from the existing execution event. An idle boundary prevents relabelling an earlier user stop. Retrying the same assistant clears that derived marker.
- The timeline labels that divider **Server restarted**, including the assistant's hover/menu metadata. A recovery synthetic supplies the divider after process death; an adjacent shutdown divider is reused. User stops and history without a recorded cause retain **Interrupted**.
- A job interrupted outside explicit cancellation carries `cancellationReason: shutdown` to its completion observer. Explicit cancellation settles first through `Job.cancel`, so it has no shutdown marker. The completion synthetic records this minimal fact in metadata and renders **General stopped by server restart**.
- No new event type, public schema, migration, or theme token. The existing warning outcome styling remains appropriate for stopped work.

## Background recovery already has a hook

`SessionRestart.resumeSuspendedSessions` reads durable `job.background/` markers. `recoverSubagent` verifies the child-parent relationship and parent existence, increments the child's durable resume budget, recreates its job, resumes the child, and delivers its completion using the original notification ID. Unrelated child claims are released. `session-execution.test.ts` already covers child recovery, result delivery without rerunning, exhausted budgets, and repeated sweeps.

For the follow-up, first compare the installed server with this implementation. Then exercise a real graceful shutdown of an isolated fixture with a running parent and multiple background children. In particular, inspect whether a shutdown-time completion observer can admit a cancelled notice and acknowledge the running recovery marker before the next process sees it. The current labels must not imply that a stopped child resumed.

Risks to verify:

- **Duplicate work:** recovery resumes durable history, but an external command may have completed before its result was persisted. At-least-once execution cannot promise exactly-once external effects.
- **Cost:** simultaneous children can repeat model work and spend on each restart. Keep the existing per-execution attempt cap and check the desired concurrency policy.
- **Missing parent:** retain the existing relationship/existence check. Do not revive deleted or unrelated children. Decide explicitly whether idle or archived parents should permit child recovery; the current hook is broader than “parent was running”.
- **Idempotency:** preserve child IDs, job IDs, and notification IDs. Repeated sweeps must join active execution; notification admission must deduplicate before acknowledging recovery state. A notification admitted during shutdown must not accidentally remove a child's only recovery marker.

**Rough size:** 1–2 engineering days to audit and verify the existing recovery path, including the shutdown-observer boundary; a fresh implementation on the captured older version would be roughly 3–5 days. No recovery expansion is included here.

## Verification

- `restart-before-after.png` renders the production session-ui story headlessly: legacy history without a cause above, shutdown-tagged history below. Both the divider and the child row are visible.
- Core job/projector tests and timeline row tests pass. `bun typecheck` passes in `core`, `client`, `session-ui`, and `ui`.
- All four headless story scenarios pass: shutdown, process death, user stop, and legacy history. A cold Pro run timed out before mounting the shutdown story; the isolated rerun passed. The live client reducer tests also pass, including a previous user-stop boundary.
- Root `bun run check` passed (36 typecheck tasks, lint with warnings and zero errors). The later live-client reducer delta received its own targeted checks.
- Production-minified timeline row construction, 1,000 turns, 100 samples on the same worker: base p50/p95 8.051/38.432 ms; changed 6.874/29.994 ms. No observed regression; this is a bounded projection benchmark, not an app startup benchmark.
