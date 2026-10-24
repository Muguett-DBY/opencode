import { describe, expect } from "bun:test"
import { DateTime, Effect, Fiber, Option, Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import { asc, eq, sql } from "drizzle-orm"
import { Database } from "@opencode/core/database/database"
import { Agent } from "@opencode/core/agent"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Bus } from "@opencode/core/bus"
import { Event } from "@opencode/schema/event"
import { EventTable } from "@opencode/core/event/sql"
import { Model } from "@opencode/core/model"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath, RelativePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { Money } from "@opencode/schema/money"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionExecution } from "@opencode/core/session/execution"
import { fromRow } from "@opencode/core/session/info"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionStore } from "@opencode/core/session/store"
import { Shell } from "@opencode/schema/shell"
import {
  InstructionStateTable,
  MediaBlobTable,
  MediaRefTable,
  SessionInboxTable,
  SessionMessageTable,
  SessionTable,
} from "@opencode/core/session/sql"
import { Image } from "@opencode/core/image"
import { Config } from "@opencode/core/config"
import { Document, Info } from "@opencode/schema/config"
import { ConfigExperimental } from "@opencode/schema/config/experimental"
import { SessionMediaBlob } from "@opencode/core/session/media-blob"
import { SessionTransfer } from "@opencode/core/session/transfer"
import { testEffect } from "./lib/effect"
import { Snapshot } from "@opencode/core/snapshot"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      SessionProjector.node,
      SessionInbox.node,
      SessionStore.node,
      Image.node,
      SessionTransfer.node,
    ]),
    [Bus.node.replace(Bus.configured({ persist: true })), SessionExecution.node.replace(SessionExecution.noopLayer)],
  ),
)
const sessionsLayer = AppNodeBuilder.build(Session.node, [SessionExecution.node.replace(SessionExecution.noopLayer)])
const sessionID = Session.ID.make("ses_projector_test")
const created = DateTime.makeUnsafe(0)
const model = { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") }
const previousModel = { ...model, variant: Model.VariantID.make("medium") }
const encodeMessage = Schema.encodeSync(SessionMessage.Info)
const build = Agent.defaultID

const assistantRow = (
  id: SessionMessage.ID,
  seq: number,
  time: { created: DateTime.Utc; completed?: DateTime.Utc } = { created },
  usage?: Pick<SessionMessage.Assistant, "cost" | "tokens">,
) => {
  const {
    id: _,
    type,
    ...data
  } = encodeMessage(
    SessionMessage.Assistant.make({ id, type: "assistant", agent: build, model, content: [], time, ...usage }),
  )
  return { id, session_id: sessionID, type, seq, time_created: DateTime.toEpochMillis(time.created), data }
}

const seedSession = (overrides?: Partial<typeof SessionTable.$inferInsert>) =>
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "test",
        directory: "/project",
        title: "test",
        version: "test",
        ...overrides,
      })
      .run()
    return db
  })

describe("SessionProjector", () => {
  it.effect("does not settle a pending manual compaction on an auto failure", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service
      const inputID = SessionMessage.ID.make("msg_manual_compaction")
      yield* inbox.admitCompaction({ id: inputID, sessionID, delivery: "queue" })

      yield* bus.publish(SessionEvent.Compaction.Failed, {
        sessionID,
        reason: "auto",
        error: { type: "compaction.failed", message: "Auto compaction failed" },
      })

      expect(yield* SessionInbox.find(db, inputID)).toMatchObject({ id: inputID })
    }),
  )

  it.effect("loads legacy revert storage into canonical state", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const legacy = JSON.stringify({
        messageID: "msg_boundary",
        snapshot: "tree",
        diff: "legacy patch",
        files: [{ path: "src/old.ts", status: "modified", additions: 1, deletions: 0, patch: "@@" }],
      })
      yield* db.run(sql`update session_v2 set revert = ${legacy} where id = ${sessionID}`)
      const stored = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      if (!stored) return yield* Effect.die("Session row missing")
      const storedRevert = fromRow(stored).revert
      expect(String(storedRevert?.messageID)).toBe("msg_boundary")
      expect(String(storedRevert?.snapshot)).toBe("tree")
      expect(storedRevert?.files).toEqual([
        { file: "src/old.ts", status: "modified", additions: 1, deletions: 0, patch: "@@" },
      ])
    }),
  )

  it.effect("projects staged, cleared, and committed reverts", () =>
    Effect.gen(function* () {
      const db = yield* seedSession({
        cost: 1.25,
        tokens_input: 10,
        tokens_output: 4,
        tokens_reasoning: 2,
        tokens_cache_read: 3,
        tokens_cache_write: 1,
      })
      const boundary = SessionMessage.ID.make("msg_boundary")
      const earlier = SessionMessage.ID.make("msg_earlier")
      yield* db
        .insert(SessionMessageTable)
        .values([
          assistantRow(earlier, 0),
          assistantRow(
            boundary,
            1,
            { created },
            {
              cost: Money.USD.make(0.5),
              tokens: { input: 4, output: 1, reasoning: 1, cache: { read: 1, write: 0 } },
            },
          ),
          assistantRow(
            SessionMessage.ID.make("msg_later"),
            2,
            { created },
            {
              cost: Money.USD.make(0.75),
              tokens: { input: 6, output: 3, reasoning: 1, cache: { read: 2, write: 1 } },
            },
          ),
        ])
        .run()
      yield* db
        .insert(InstructionStateTable)
        .values({
          session_id: sessionID,
          epoch_start: 0,
          through_seq: 0,
          initial_values: {},
          current_values: {},
        })
        .run()
      const bus = yield* Bus.Service
      yield* bus.publish(SessionEvent.RevertEvent.Staged, {
        sessionID,
        revert: { messageID: boundary, snapshot: Snapshot.ID.make("tree"), files: [] },
      })
      expect((yield* db.select({ revert: SessionTable.revert }).from(SessionTable).get())?.revert).toMatchObject({
        messageID: boundary,
        snapshot: "tree",
        files: [],
      })
      yield* bus.publish(SessionEvent.RevertEvent.Cleared, { sessionID })
      expect((yield* db.select({ revert: SessionTable.revert }).from(SessionTable).get())?.revert).toBeNull()
      yield* bus.publish(SessionEvent.RevertEvent.Staged, {
        sessionID,
        revert: { messageID: boundary, files: [] },
      })
      yield* bus.publish(SessionEvent.RevertEvent.Committed, {
        sessionID,
        to: boundary,
      })
      expect(
        (yield* db.select({ id: SessionMessageTable.id }).from(SessionMessageTable).all()).map((row) => row.id),
      ).toEqual([earlier])
      expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()).toMatchObject({
        cost: Money.USD.make(1.25),
        tokens_input: 10,
        tokens_output: 4,
        tokens_reasoning: 2,
        tokens_cache_read: 3,
        tokens_cache_write: 1,
      })
      // No instruction update followed the boundary, so the fold survives.
      expect(yield* db.select().from(InstructionStateTable).get().pipe(Effect.orDie)).toMatchObject({ through_seq: 0 })
    }),
  )

  it.effect("orders projected messages and context by durable aggregate sequence", () =>
    Effect.gen(function* () {
      yield* seedSession()
      const bus = yield* Bus.Service

      yield* bus.publish(SessionEvent.InboxEnqueued, {
        sessionID,
        inboxID: SessionMessage.ID.make("msg_first"),
        item: { type: "user", payload: { text: "first" }, delivery: "steer" },
      })
      yield* bus.publish(
        SessionEvent.InboxDelivered,
        {
          sessionID,
          inboxID: SessionMessage.ID.make("msg_first"),
        },
        { id: Event.ID.make("evt_z") },
      )
      yield* bus.publish(SessionEvent.InboxEnqueued, {
        sessionID,
        inboxID: SessionMessage.ID.make("msg_second"),
        item: { type: "user", payload: { text: "second" }, delivery: "steer" },
      })
      yield* bus.publish(
        SessionEvent.InboxDelivered,
        {
          sessionID,
          inboxID: SessionMessage.ID.make("msg_second"),
        },
        { id: Event.ID.make("evt_a") },
      )

      const sessions = yield* Session.Service
      const firstPage = yield* sessions.messages({ sessionID, limit: 1, order: "asc" })
      expect(firstPage.map((message) => (message.type === "user" ? message.text : message.type))).toEqual(["first"])
      const secondPage = yield* sessions.messages({
        sessionID,
        limit: 1,
        order: "asc",
        cursor: { id: firstPage[0]!.id, direction: "next" },
      })
      expect(secondPage.map((message) => (message.type === "user" ? message.text : message.type))).toEqual(["second"])
      expect(
        (yield* sessions.messages({
          sessionID,
          limit: 1,
          order: "asc",
          cursor: { id: secondPage[0]!.id, direction: "previous" },
        })).map((message) => (message.type === "user" ? message.text : message.type)),
      ).toEqual(["first"])
      expect(
        (yield* sessions.context(sessionID)).map((message) => (message.type === "user" ? message.text : message.type)),
      ).toEqual(["first", "second"])
    }).pipe(Effect.provide(sessionsLayer)),
  )

  it.effect("maps malformed persisted rows consistently while single-message lookup defects", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const messageID = SessionMessage.ID.make("msg_malformed")
      yield* db
        .insert(SessionMessageTable)
        .values({
          id: messageID,
          session_id: sessionID,
          type: "user",
          seq: 0,
          data: { text: "valid before corruption", time: { created: 0 } },
        })
        .run()
        .pipe(Effect.orDie)
      yield* db.run(sql`update session_message set data = '{"time":{"created":0}}' where id = ${messageID}`)

      const sessions = yield* Session.Service
      const store = yield* SessionStore.Service
      const expected = { _tag: "Session.MessageDecodeError", sessionID, messageID }
      expect(yield* store.messages({ sessionID }).pipe(Effect.flip)).toMatchObject(expected)
      expect(yield* sessions.messages({ sessionID }).pipe(Effect.flip)).toMatchObject(expected)
      expect(yield* sessions.context(sessionID).pipe(Effect.flip)).toMatchObject(expected)
      expect(yield* sessions.message({ sessionID, messageID }).pipe(Effect.catchDefect(Effect.succeed))).toMatchObject(
        expected,
      )
    }).pipe(Effect.provide(sessionsLayer)),
  )

  it.effect("checks session existence before resolving a missing message cursor", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const missing = Session.ID.make("ses_missing")
      expect(
        yield* sessions
          .messages({
            sessionID: missing,
            cursor: { id: SessionMessage.ID.make("msg_missing"), direction: "next" },
          })
          .pipe(Effect.flip),
      ).toEqual(new Session.NotFoundError({ sessionID: missing }))
    }).pipe(Effect.provide(sessionsLayer)),
  )

  it.effect("consumes the pending row and projects the message at promotion", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service
      const id = SessionMessage.ID.make("msg_admitted")
      const admitted = yield* inbox.admit({
        id,
        sessionID,
        item: { type: "user", payload: { text: "promote me" }, delivery: "steer" },
      })
      if (!admitted) return yield* Effect.die("Prompt admission failed")

      const event = yield* bus.publish(SessionEvent.InboxDelivered, {
        sessionID,
        inboxID: id,
      })

      expect(
        yield* db.select().from(SessionInboxTable).where(eq(SessionInboxTable.id, id)).get().pipe(Effect.orDie),
      ).toBeUndefined()
      expect(
        yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ session_id: sessionID, type: "user", seq: event.durable?.seq })
    }),
  )

  it.effect("projects durable context messages supported by the updater", () =>
    Effect.gen(function* () {
      const db = yield* seedSession({ agent: Agent.ID.make("plan"), model: previousModel })
      const bus = yield* Bus.Service

      yield* bus.publish(SessionEvent.AgentSelected, {
        sessionID,
        agent: build,
      })
      yield* bus.publish(SessionEvent.ModelSelected, {
        sessionID,
        model,
      })
      yield* bus.publish(SessionEvent.Synthetic, {
        sessionID,
        text: "synthetic context",
        metadata: { source: "projector-test" },
      })
      // Serialized events have no transient envelope metadata to supply the background marker.
      yield* bus.replay({
        id: Event.ID.create(),
        created: 0,
        aggregateID: sessionID,
        seq: 3,
        type: Bus.versionedType(SessionEvent.Shell.Started.type, 1),
        data: {
          sessionID,
          shell: Shell.Info.make({
            id: Shell.ID.make("sh_projector"),
            status: "running",
            command: "pwd",
            cwd: "/project",
            shell: "/bin/sh",
            file: "/tmp/sh_projector.out",
            metadata: { background: true },
            time: { started: 0 },
          }),
        },
      })
      yield* bus.publish(SessionEvent.Shell.Ended, {
        sessionID,
        shell: Shell.Info.make({
          id: Shell.ID.make("sh_projector"),
          status: "exited",
          command: "pwd",
          cwd: "/project",
          shell: "/bin/sh",
          file: "/tmp/sh_projector.out",
          exit: 0,
          metadata: {},
          time: { started: 0, completed: 1 },
        }),
        output: { output: "/project", cursor: 8, size: 8, truncated: false },
      })
      yield* bus.publish(SessionEvent.Compaction.Started, {
        sessionID,
        reason: "manual",
      })
      yield* bus.publish(SessionEvent.Compaction.Delta, {
        sessionID,
        text: "partial",
      })
      expect(
        yield* db
          .select({ id: EventTable.id })
          .from(EventTable)
          .where(sql`${EventTable.type} like 'session.compaction.delta.%'`)
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(0)
      expect(
        yield* db
          .select({ data: SessionMessageTable.data })
          .from(SessionMessageTable)
          .where(eq(SessionMessageTable.type, "compaction"))
          .all()
          .pipe(Effect.orDie),
      ).toEqual([{ data: expect.objectContaining({ status: "running", summary: "" }) }])
      yield* bus.publish(SessionEvent.Compaction.Ended, {
        sessionID,
        reason: "manual",
        text: "summary",
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.seq))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type }),
      )

      expect(messages.map((message) => message.type)).toEqual([
        "agent-switched",
        "model-switched",
        "synthetic",
        "shell",
        "compaction",
      ])
      expect(messages.find((message) => message.type === "synthetic")).toMatchObject({
        text: "synthetic context",
        metadata: { source: "projector-test" },
      })
      expect(messages.find((message) => message.type === "agent-switched")).toMatchObject({
        agent: build,
        previous: "plan",
      })
      expect(messages.find((message) => message.type === "model-switched")).toMatchObject({ previous: previousModel })
      expect(messages.find((message) => message.type === "shell")).toMatchObject({
        command: "pwd",
        status: "exited",
        exit: 0,
        metadata: { background: true },
        output: { output: "/project", truncated: false },
        time: { completed: DateTime.makeUnsafe(0) },
      })
      expect(messages.find((message) => message.type === "compaction")).toMatchObject({ summary: "summary" })
      expect(rows.find((row) => row.type === "compaction")?.data).not.toHaveProperty("recent")
      expect(
        yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get().pipe(Effect.orDie),
      ).toMatchObject({
        agent: "build",
        model,
        time_updated: DateTime.toEpochMillis(created),
      })
    }),
  )

  it.effect("rejects distinct creator events that reuse one projected message ID", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const id = SessionMessage.ID.make("msg_creator_collision")
      const { id: _, type, ...data } = encodeMessage({ id, type: "synthetic", text: "existing", time: { created } })
      yield* db
        .insert(SessionMessageTable)
        .values({ id, session_id: sessionID, type, seq: 0, time_created: 0, data })
        .run()

      const exit = yield* bus
        .publish(SessionEvent.Step.Started, {
          sessionID,
          assistantMessageID: id,
          agent: build,
          model,
          started: 0,
        })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect(
        yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.id, id)).get().pipe(Effect.orDie),
      ).toMatchObject({ type: "synthetic" })
    }),
  )

  it.effect("projects retry state and clears it at the next step or execution terminal", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const first = SessionMessage.ID.make("msg_retry_first")
      const second = SessionMessage.ID.make("msg_retry_second")
      yield* bus.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID: first,
        agent: build,
        model,
        started: 0,
      })
      yield* bus.publish(SessionEvent.RetryScheduled, {
        sessionID,
        assistantMessageID: first,
        attempt: 2,
        at: 2_000,
        error: { type: "provider.transport", message: "Disconnected" },
      })

      const decode = (row: typeof SessionMessageTable.$inferSelect) =>
        Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type })
      const firstRow = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.id, first))
        .get()
        .pipe(Effect.orDie)
      const projected = firstRow ?? (yield* Effect.die(new Error("Missing retry projection")))
      expect(decode(projected)).toMatchObject({
        retry: { attempt: 2, at: DateTime.makeUnsafe(2_000), error: { type: "provider.transport" } },
      })

      yield* bus.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID: second,
        agent: build,
        model,
        started: 0,
      })
      yield* bus.publish(SessionEvent.RetryScheduled, {
        sessionID,
        assistantMessageID: second,
        attempt: 3,
        at: 6_000,
        error: { type: "provider.internal", message: "Unavailable" },
      })
      yield* bus.publish(SessionEvent.Execution.Interrupted, { sessionID, reason: "shutdown" })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.seq))
        .all()
        .pipe(Effect.orDie)
      expect(decode(rows[0])).not.toHaveProperty("retry")
      expect(decode(rows[1])).not.toHaveProperty("retry")
    }),
  )

  it.effect("does not infer restart continuation from lifecycle history", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const suspended = () =>
        db
          .select({ timeSuspended: SessionTable.time_suspended })
          .from(SessionTable)
          .where(eq(SessionTable.id, sessionID))
          .get()
          .pipe(Effect.orDie)

      yield* bus.publish(SessionEvent.Execution.Interrupted, { sessionID, reason: "shutdown" })
      expect((yield* suspended())?.timeSuspended).toBeNull()

      yield* bus.publish(SessionEvent.Execution.Started, { sessionID })
      expect((yield* suspended())?.timeSuspended).toBeNull()
    }),
  )

  it.effect("updates only the newest incomplete assistant projection", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      yield* db
        .insert(SessionMessageTable)
        .values([
          assistantRow(SessionMessage.ID.make("msg_assistant_1"), 0),
          assistantRow(SessionMessage.ID.make("msg_assistant_2"), 1),
        ])
        .run()
        .pipe(Effect.orDie)

      const service = yield* Bus.Service
      const usageUpdated = yield* service
        .subscribe(SessionEvent.UsageUpdated)
        .pipe(Stream.runHead, Effect.forkScoped({ startImmediately: true }))
      yield* service.publish(SessionEvent.Step.Ended, {
        sessionID,
        assistantMessageID: SessionMessage.ID.make("msg_assistant_2"),
        finish: "stop",
        cost: Money.USD.make(1.25),
        tokens: { input: 10, output: 4, reasoning: 2, cache: { read: 3, write: 1 } },
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.id))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type }),
      )
      expect(messages[0]).not.toHaveProperty("time.completed")
      expect(messages[1]).toMatchObject({
        type: "assistant",
        finish: "stop",
        cost: Money.USD.make(1.25),
        tokens: { input: 10, output: 4, reasoning: 2, cache: { read: 3, write: 1 } },
        time: { completed: DateTime.makeUnsafe(0) },
      })
      expect(
        yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get().pipe(Effect.orDie),
      ).toMatchObject({
        cost: 1.25,
        tokens_input: 10,
        tokens_output: 4,
        tokens_reasoning: 2,
        tokens_cache_read: 3,
        tokens_cache_write: 1,
      })
      expect(Option.getOrThrow(yield* Fiber.join(usageUpdated)).data).toEqual({
        sessionID,
        cost: Money.USD.make(1.25),
        tokens: { input: 10, output: 4, reasoning: 2, cache: { read: 3, write: 1 } },
      })
    }),
  )

  it.effect("reads and replays native data stored under its pre-v2.1 names", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const store = yield* SessionStore.Service
      const bus = yield* Bus.Service
      const assistantID = SessionMessage.ID.make("msg_legacy_assistant")
      const {
        id: _,
        type: __,
        ...assistant
      } = encodeMessage(
        SessionMessage.Assistant.make({
          id: assistantID,
          type: "assistant",
          agent: build,
          model,
          content: [],
          time: { created },
        }),
      )
      const legacyAssistant = {
        ...assistant,
        providerState: { step: "kept" },
        content: [
          { type: "text", text: "hi", state: { signature: "text" } },
          { type: "reasoning", text: "thinking", state: { signature: "reasoning" } },
          {
            type: "tool",
            id: "call",
            name: "search",
            executed: true,
            providerState: { call: "kept" },
            providerResultState: { result: "kept" },
            state: { status: "completed", input: {}, content: [{ type: "text", text: "done" }] },
            time: { created: 0, ran: 0, completed: 0 },
          },
        ],
      }
      const legacyCompaction = {
        status: "completed",
        reason: "manual",
        summary: "summary",
        providerState: { compaction: "kept" },
        time: { created: 0 },
      }
      yield* db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES
          (${assistantID}, ${sessionID}, 'assistant', 0, 0, 0, ${JSON.stringify(legacyAssistant)}),
          ('msg_legacy_compaction', ${sessionID}, 'compaction', 1, 0, 0, ${JSON.stringify(legacyCompaction)})
      `)
      const stored = [
        {
          id: Event.ID.make("evt_legacy_0"),
          type: "session.tool.called.1",
          data: { id: "call", input: {}, executed: true, state: { call: "kept" } },
        },
        {
          id: Event.ID.make("evt_legacy_1"),
          type: "session.tool.success.2",
          data: {
            id: "call",
            content: [{ type: "text", text: "done" }],
            executed: true,
            resultState: { result: "kept" },
          },
        },
      ].map((event, seq) => ({
        ...event,
        seq,
        created: 0,
        aggregateID: sessionID,
        data: { sessionID, assistantMessageID: assistantID, ...event.data },
      }))
      yield* db.run(sql`INSERT OR IGNORE INTO event_sequence (aggregate_id, seq) VALUES (${sessionID}, 1)`)
      for (const event of stored)
        yield* db.run(
          sql`INSERT INTO event (id, aggregate_id, seq, created, type, data) VALUES (${event.id}, ${sessionID}, ${event.seq}, 0, ${event.type}, ${JSON.stringify(event.data)})`,
        )

      const [readAssistant, readCompaction] = yield* store.messages({ sessionID, order: "asc" })
      expect(readAssistant).toMatchObject({
        native: { step: "kept" },
        content: [
          { type: "text", native: { signature: "text" } },
          { type: "reasoning", native: { signature: "reasoning" } },
          { type: "tool", native: { call: "kept", result: "kept" } },
        ],
      })
      expect(readCompaction).toMatchObject({ native: { compaction: "kept" } })

      expect(
        (yield* Stream.runCollect(bus.log({ aggregateID: sessionID }))).filter((item) => !Bus.isSynced(item)),
      ).toMatchObject([
        { type: "session.tool.called", data: { native: { call: "kept" } } },
        { type: "session.tool.success", data: { native: { result: "kept" } } },
      ])

      // Re-delivering an event that is already stored in its legacy shape stays idempotent.
      yield* Effect.forEach(stored, (event) => bus.replay(event), { discard: true })

      // A legacy-shaped event replayed from elsewhere is projected and stored in the current shape.
      yield* bus.replay({
        id: Event.ID.make("evt_legacy_2"),
        type: "session.step.ended.1",
        seq: 2,
        created: 0,
        aggregateID: sessionID,
        data: {
          sessionID,
          assistantMessageID: assistantID,
          finish: "stop",
          providerState: { step: "replayed" },
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        },
      })
      const replayed = yield* db.get<{ data: string }>(sql`SELECT data FROM event WHERE id = 'evt_legacy_2'`)
      expect(JSON.parse(replayed?.data ?? "{}")).toMatchObject({ native: { step: "replayed" } })
      expect(JSON.parse(replayed?.data ?? "{}")).not.toHaveProperty("providerState")

      // Projecting onto the legacy row wrote it back in the current shape.
      const rewritten = JSON.parse(
        (yield* db.get<{ data: string }>(sql`SELECT data FROM session_message WHERE id = ${assistantID}`))?.data ??
          "{}",
      )
      expect(rewritten).not.toHaveProperty("providerState")
      expect(rewritten).toMatchObject({
        native: { step: "replayed" },
        content: [
          { type: "text", native: { signature: "text" } },
          { type: "reasoning", native: { signature: "reasoning" } },
          { type: "tool", native: { call: "kept", result: "kept" } },
        ],
      })
      expect(rewritten.content.map((part: Record<string, unknown>) => Object.keys(part).toSorted())).toEqual([
        ["native", "text", "type"],
        ["native", "text", "type"],
        ["executed", "id", "name", "native", "state", "time", "type"],
      ])
    }),
  )

  it.effect("merges tool result native data into the call's native data", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const assistantMessageID = SessionMessage.ID.make("msg_tools")
      yield* db.insert(SessionMessageTable).values(assistantRow(assistantMessageID, 0)).run().pipe(Effect.orDie)

      const service = yield* Bus.Service
      const call = (id: string) =>
        Effect.gen(function* () {
          yield* service.publish(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id, name: id })
          yield* service.publish(SessionEvent.Tool.Called, {
            sessionID,
            assistantMessageID,
            id,
            input: {},
            executed: true,
            native: { call: id },
          })
        })
      yield* call("succeeded")
      yield* service.publish(SessionEvent.Tool.Success, {
        sessionID,
        assistantMessageID,
        id: "succeeded",
        content: [{ type: "text", text: "done" }],
        executed: true,
        native: { result: "succeeded" },
      })
      yield* call("failed")
      yield* service.publish(SessionEvent.Tool.Failed, {
        sessionID,
        assistantMessageID,
        id: "failed",
        error: { type: "tool.execution", message: "failed" },
        executed: true,
      })

      const row = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.id, assistantMessageID))
        .get()
        .pipe(Effect.orDie)
      expect(row?.data).toMatchObject({
        content: [
          { id: "succeeded", native: { call: "succeeded", result: "succeeded" }, state: { status: "completed" } },
          { id: "failed", native: { call: "failed" }, state: { status: "error" } },
        ],
      })
    }),
  )

  it.effect("projects ended and failed step terminal state", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const endedID = SessionMessage.ID.make("msg_ended")
      const failedID = SessionMessage.ID.make("msg_failed")
      yield* db
        .insert(SessionMessageTable)
        .values([assistantRow(endedID, 0), assistantRow(failedID, 1)])
        .run()
        .pipe(Effect.orDie)

      const service = yield* Bus.Service
      yield* service.publish(SessionEvent.Step.Streamed, {
        sessionID,
        assistantMessageID: endedID,
      })
      yield* service.publish(SessionEvent.Step.Ended, {
        sessionID,
        assistantMessageID: endedID,
        finish: "stop",
        rawFinish: "stop_sequence",
        native: { response: "ended" },
        cost: Money.USD.make(1),
        tokens: { input: 2, output: 3, reasoning: 4, cache: { read: 5, write: 6 } },
        snapshot: Snapshot.ID.make("snap_ended"),
        files: [RelativePath.make("src/ended.ts")],
      })
      yield* service.publish(SessionEvent.Step.Failed, {
        sessionID,
        assistantMessageID: failedID,
        finish: "content-filter",
        rawFinish: "blocked",
        native: { response: "failed" },
        error: { type: "provider.invalid-request", message: "Failed" },
        snapshot: Snapshot.ID.make("snap_failed"),
        files: [RelativePath.make("src/failed.ts")],
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.seq))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type }),
      )
      expect(messages[0]).toMatchObject({
        type: "assistant",
        finish: "stop",
        rawFinish: "stop_sequence",
        native: { response: "ended" },
        cost: Money.USD.make(1),
        tokens: { input: 2, output: 3, reasoning: 4, cache: { read: 5, write: 6 } },
        snapshot: { end: "snap_ended", files: ["src/ended.ts"] },
        time: { streamed: created, completed: created },
      })
      expect(messages[1]).toMatchObject({
        type: "assistant",
        finish: "content-filter",
        rawFinish: "blocked",
        native: { response: "failed" },
        error: { type: "provider.invalid-request", message: "Failed" },
        snapshot: { end: "snap_failed", files: ["src/failed.ts"] },
        time: { completed: created },
      })
    }),
  )

  it.effect("does not revive a stale incomplete assistant projection", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      yield* db
        .insert(SessionMessageTable)
        .values([
          assistantRow(SessionMessage.ID.make("msg_assistant_stale"), 0),
          assistantRow(SessionMessage.ID.make("msg_assistant_completed"), 1, {
            created: DateTime.makeUnsafe(1),
            completed: DateTime.makeUnsafe(2),
          }),
        ])
        .run()
        .pipe(Effect.orDie)

      const service = yield* Bus.Service
      yield* service.publish(SessionEvent.Text.Started, {
        sessionID,
        assistantMessageID: SessionMessage.ID.make("msg_assistant_completed"),
        ordinal: 0,
      })

      const rows = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .orderBy(asc(SessionMessageTable.id))
        .all()
        .pipe(Effect.orDie)
      const messages = rows.map((row) =>
        Schema.decodeUnknownSync(SessionMessage.Info)({ ...row.data, id: row.id, type: row.type }),
      )
      expect(messages).toEqual([
        SessionMessage.Assistant.make({
          id: SessionMessage.ID.make("msg_assistant_completed"),
          type: "assistant",
          agent: build,
          model,
          content: [SessionMessage.AssistantText.make({ type: "text", text: "" })],
          time: { created: DateTime.makeUnsafe(1), completed: DateTime.makeUnsafe(2) },
        }),
        SessionMessage.Assistant.make({
          id: SessionMessage.ID.make("msg_assistant_stale"),
          type: "assistant",
          agent: build,
          model,
          content: [],
          time: { created },
        }),
      ])
    }),
  )

  it.effect("bumps session time_updated on step lifecycle events", () =>
    Effect.gen(function* () {
      const db = yield* seedSession({ time_created: 0, time_updated: 0 })
      const bus = yield* Bus.Service
      const updated = () =>
        db.select({ time_updated: SessionTable.time_updated }).from(SessionTable).get().pipe(Effect.orDie)
      const first = SessionMessage.ID.make("msg_touch_first")
      const second = SessionMessage.ID.make("msg_touch_second")

      yield* TestClock.setTime(5)
      yield* bus.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID: first,
        agent: build,
        model,
        started: 5,
      })
      expect(yield* updated()).toEqual({ time_updated: 5 })

      yield* TestClock.setTime(9)
      yield* bus.publish(SessionEvent.Step.Ended, {
        sessionID,
        assistantMessageID: first,
        finish: "stop",
        cost: Money.USD.make(0),
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })
      expect(yield* updated()).toEqual({ time_updated: 9 })

      yield* TestClock.setTime(12)
      yield* bus.publish(SessionEvent.Step.Started, {
        sessionID,
        assistantMessageID: second,
        agent: build,
        model,
        started: 12,
      })
      yield* TestClock.setTime(15)
      yield* bus.publish(SessionEvent.Step.Failed, {
        sessionID,
        assistantMessageID: second,
        error: { type: "provider.invalid-request", message: "Failed" },
      })
      expect(yield* updated()).toEqual({ time_updated: 15 })
    }),
  )

  it.effect("gates media_blob ref writes behind experimental.mediaBlob (default off) while always hydrating reads", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service
      const store = yield* SessionStore.Service

      const rawBytes = Buffer.from("default-off-inline-payload")
      const base64 = rawBytes.toString("base64")
      const inlineID = SessionMessage.ID.make("msg_default_inline")
      const file = {
        data: base64,
        mime: "application/pdf",
        source: { type: "inline" as const },
        name: "inline.pdf",
      }

      // Default (experimental.mediaBlob off): writes stay inline and media_blob/media_ref stay empty.
      yield* inbox.admit({
        id: inlineID,
        sessionID,
        item: { type: "user", payload: { text: "keep inline", files: [file] }, delivery: "steer" },
      })
      yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID: inlineID })

      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(0)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(0)
      const rawInlineRow = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.id, inlineID))
        .get()
        .pipe(Effect.orDie)
      expect(JSON.stringify(rawInlineRow?.data)).toContain(base64)

      // When experimental.mediaBlob is enabled, writes externalize and deduplicate into media_blob + media_ref.
      const userID = SessionMessage.ID.make("msg_user_media_blob")
      const item = {
        type: "user" as const,
        payload: { text: "check these", files: [file, { ...file, name: "spec-copy.pdf" }] },
        delivery: "steer" as const,
      }

      const admitted = yield* inbox
        .admit({ id: userID, sessionID, item })
        .pipe(SessionMediaBlob.withWriteEnabled(true))
      expect(admitted?.payload.files).toEqual([file, { ...file, name: "spec-copy.pdf" }])

      const rawInbox = yield* db
        .select()
        .from(SessionInboxTable)
        .where(eq(SessionInboxTable.id, userID))
        .get()
        .pipe(Effect.orDie)
      expect(JSON.stringify(rawInbox?.payload)).not.toContain(base64)
      expect(JSON.stringify(rawInbox?.payload)).toContain(`"media:${SessionMediaBlob.hashBytes(rawBytes)}"`)

      // Promotion preserves externalized refs even when WriteEnabled is not set on the delivery fiber.
      yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID: userID })

      const assistantMessageID = SessionMessage.ID.make("msg_assistant_media_blob")
      const dataUri = `data:application/pdf;base64,${base64}`
      yield* Effect.gen(function* () {
        yield* bus.publish(SessionEvent.Step.Started, {
          sessionID,
          assistantMessageID,
          agent: build,
          model,
          started: 1,
        })
        yield* bus.publish(SessionEvent.Tool.Input.Started, {
          sessionID,
          assistantMessageID,
          id: "call_media_1",
          name: "read",
        })
        yield* bus.publish(SessionEvent.Tool.Called, {
          sessionID,
          assistantMessageID,
          id: "call_media_1",
          input: { path: "spec.pdf" },
          executed: false,
        })
        yield* bus.publish(SessionEvent.Tool.Success, {
          sessionID,
          assistantMessageID,
          id: "call_media_1",
          executed: false,
          content: [
            { type: "text", text: "PDF read" },
            { type: "file", uri: dataUri, mime: "application/pdf", name: "spec.pdf" },
            { type: "file", uri: dataUri, mime: "application/pdf", name: "spec-2.pdf" },
          ],
        })
      }).pipe(SessionMediaBlob.withWriteEnabled(true))

      const blobs = yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)
      expect(blobs).toHaveLength(1)
      expect(blobs[0]?.mime).toBe("application/pdf")
      expect(blobs[0]?.bytes).toBe(rawBytes.byteLength)
      expect(blobs[0]?.width).toBeNull()
      expect(blobs[0]?.height).toBeNull()
      expect(Buffer.from(blobs[0]!.data).equals(rawBytes)).toBe(true)

      const refs = yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)
      expect(refs).toHaveLength(2)

      // Stored rows name the blob by hash, never by a server route.
      const hash = SessionMediaBlob.hashBytes(rawBytes)
      expect(blobs[0]?.hash).toBe(hash)
      const rawRows = JSON.stringify([
        yield* db.select().from(SessionMessageTable).all().pipe(Effect.orDie),
        yield* db.select().from(SessionInboxTable).all().pipe(Effect.orDie),
      ])
      expect(rawRows).toContain(`"media:${hash}"`)
      expect(rawRows).not.toContain("/api/")

      // Hydrate-on-read works even when experimental.mediaBlob is off.
      expect((yield* store.message(userID))?.message).toMatchObject({
        id: userID,
        type: "user",
        files: [file, { ...file, name: "spec-copy.pdf" }],
      })

      // Rows written by early builds carry `/api/media/<hash>`; reads still resolve them.
      const legacyID = SessionMessage.ID.make("msg_legacy_media_ref")
      yield* db
        .insert(SessionMessageTable)
        .values({
          id: legacyID,
          session_id: sessionID,
          type: "user",
          seq: 1_000,
          time_created: 1_000,
          data: {
            text: "legacy ref",
            files: [{ uri: `/api/media/${hash}`, mime: "application/pdf", bytes: rawBytes.byteLength }] as any,
            time: { created: 1_000 },
          },
        })
        .run()
        .pipe(Effect.orDie)
      expect((yield* store.message(legacyID))?.message).toMatchObject({
        files: [{ data: base64, mime: "application/pdf", source: { type: "inline" } }],
      })

      expect(SessionMediaBlob.hashFromUri(`media:${hash}`)).toBe(hash)
      expect(SessionMediaBlob.hashFromUri(`/api/media/${hash}`)).toBe(hash)
      expect(SessionMediaBlob.hashFromUri(`/api/experimental/media/${hash}`)).toBe(hash)
      expect(SessionMediaBlob.hashFromUri(`https://studio.example:4096/api/experimental/media/${hash}?x=1`)).toBe(hash)
      expect(SessionMediaBlob.hashFromUri("media:../etc/passwd")).toBeUndefined()
      expect(SessionMediaBlob.hashFromUri(`media:${hash.toUpperCase()}`)).toBeUndefined()
      expect(SessionMediaBlob.hashFromUri(`/proxy/api/media/${hash}`)).toBeUndefined()
      expect(SessionMediaBlob.hashFromUri(`file:///api/media/${hash}`)).toBeUndefined()
      expect(SessionMediaBlob.hashFromUri(`/api/media/${hash}/extra`)).toBeUndefined()
    }),
  )

  it.effect("canonicalizes media URLs to media:<hash> on every write, even with experimental.mediaBlob off", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bytes = Buffer.from("canonical-blob")
      const hash = SessionMediaBlob.hashBytes(bytes)
      const gone = "d".repeat(64)
      yield* db
        .insert(MediaBlobTable)
        .values({ hash, mime: "image/png", bytes: bytes.byteLength, width: null, height: null, data: bytes })
        .run()
        .pipe(Effect.orDie)
      const urls = [
        `/api/media/${hash}`,
        `/api/experimental/media/${hash}`,
        `http://127.0.0.1:4096/api/experimental/media/${hash}`,
        `https://studio.example/api/media/${hash}?location%5Bdirectory%5D=%2Fp`,
      ]
      const owner = { sessionID, messageID: SessionMessage.ID.make("msg_canonical_tool"), seq: 1 }
      const assistant = {
        agent: build,
        model,
        content: [
          {
            type: "tool",
            id: "call_1",
            name: "fetch",
            executed: true,
            state: {
              status: "completed",
              input: {},
              content: [...urls, `/api/experimental/media/${gone}`].map((uri) => ({ type: "file", uri, mime: "image/png" })),
            },
            time: { created: 1, ran: 1, completed: 1 },
          },
        ],
        time: { created: 1, completed: 1 },
      } as any
      const user = {
        text: "imported",
        files: urls.map((uri) => ({ uri, mime: "image/png" })),
        time: { created: 2 },
      } as any

      const writtenAssistant = yield* SessionMediaBlob.externalize(db, assistant, owner)
      const writtenUser = yield* SessionMediaBlob.externalize(db, user, {
        ...owner,
        messageID: SessionMessage.ID.make("msg_canonical_user"),
        seq: 2,
      })
      const written = JSON.stringify([writtenAssistant, writtenUser])
      expect(written).not.toContain("/api/")
      expect(written.match(new RegExp(`"media:${hash}"`, "g"))).toHaveLength(urls.length * 2)
      // A blob that is already gone still canonicalizes, but records no ref (media_ref.hash references media_blob).
      expect(written).toContain(`"media:${gone}"`)
      const refs = yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)
      expect(refs.map((row) => [row.message_id, row.hash]).sort()).toEqual([
        ["msg_canonical_tool", hash],
        ["msg_canonical_user", hash],
      ])
      // Canonicalization never stores bytes; only an enabled write does.
      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(1)
    }),
  )

  it.effect("re-reads experimental.mediaBlob from config on every write, so turning it off stops externalizing", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bytes = Buffer.from("runtime-toggle")
      const data = {
        text: "toggle",
        files: [{ data: bytes.toString("base64"), mime: "application/pdf", source: { type: "inline" } }],
        time: { created: 1 },
      } as any
      const config = yield* Config.Test
      const enabled = new Document({ type: "document", info: new Info({ experimental: new ConfigExperimental.Info({ mediaBlob: true }) }) })

      // Absent from config means off.
      expect(JSON.stringify(yield* SessionMediaBlob.externalize(db, data))).toContain(bytes.toString("base64"))

      yield* config.setEntries([enabled])
      expect(yield* SessionMediaBlob.isWriteEnabled).toBe(true)
      expect(JSON.stringify(yield* SessionMediaBlob.externalize(db, data))).toContain(
        `"media:${SessionMediaBlob.hashBytes(bytes)}"`,
      )

      yield* config.setEntries([])
      expect(yield* SessionMediaBlob.isWriteEnabled).toBe(false)
      expect(JSON.stringify(yield* SessionMediaBlob.externalize(db, data))).toContain(bytes.toString("base64"))
    }).pipe(Effect.provide(Config.testLayer())),
  )

  it.effect("garbage-collects media_ref and media_blob across cancel, revert, fork, and session delete", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service

      const makeFile = (label: string) => ({
        data: Buffer.from(`media-gc-${label}`).toString("base64"),
        mime: "application/pdf",
        source: { type: "inline" as const },
        name: `${label}.pdf`,
      })

      const msg1 = SessionMessage.ID.make("msg_gc_1")
      const msg2 = SessionMessage.ID.make("msg_gc_2")
      const cancelledID = SessionMessage.ID.make("msg_gc_cancel")

      yield* Effect.gen(function* () {
        yield* inbox.admit({
          id: msg1,
          sessionID,
          item: { type: "user", payload: { text: "first", files: [makeFile("first")] }, delivery: "steer" },
        })
        yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID: msg1 })

        yield* inbox.admit({
          id: msg2,
          sessionID,
          item: { type: "user", payload: { text: "second", files: [makeFile("second")] }, delivery: "steer" },
        })
        yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID: msg2 })

        yield* inbox.admit({
          id: cancelledID,
          sessionID,
          item: { type: "user", payload: { text: "cancelled", files: [makeFile("cancelled")] }, delivery: "queue" },
        })
      }).pipe(SessionMediaBlob.withWriteEnabled(true))

      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(3)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(3)

      // 1. Cancelling a queued inbox item removes its ref and sweeps its blob.
      yield* inbox.cancel({ id: cancelledID, sessionID })
      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(2)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(2)

      // 2. Forking through msg1 records a media_ref for the child session on the first blob.
      const childSessionID = Session.ID.make("ses_gc_fork_child")
      yield* bus.publish(SessionEvent.Forked, {
        sessionID: childSessionID,
        parentID: sessionID,
        boundary: { type: "through", messageID: msg1 },
      })
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(3)

      // 3. Reverting the parent session at msg2 drops msg2's ref and sweeps the second blob.
      yield* bus.publish(SessionEvent.RevertEvent.Committed, {
        sessionID,
        to: msg2,
      })
      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(1)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(2)

      // 4. Deleting the parent session removes its ref, but the shared blob survives for the fork child.
      yield* bus.publish(SessionEvent.Deleted, { sessionID })
      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(1)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(1)

      // 5. Deleting the fork child removes the last ref and sweeps the remaining blob.
      yield* bus.publish(SessionEvent.Deleted, { sessionID: childSessionID })
      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(0)
      expect(yield* db.select().from(MediaRefTable).all().pipe(Effect.orDie)).toHaveLength(0)
    }),
  )

  it.effect("proves byte-identical toEqual output across message.list, inbox.list, and export (including >2000px and PNG->JPEG)", () =>
    Effect.gen(function* () {
      yield* seedSession({ time_created: 0, time_updated: 0 })
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service
      const store = yield* SessionStore.Service
      const transfer = yield* SessionTransfer.Service
      const image = yield* Image.Service
      const db = (yield* Database.Service).db

      const photon = yield* Effect.promise(() => import("@silvia-odwyer/photon-node"))

      // 1) Image larger than 2000px (2400x1200) -> normalized by Image.Service before persistence.
      const overW = 2400
      const overH = 1200
      const overPixels = new Uint8Array(overW * overH * 4)
      overPixels.fill(210)
      const overImg = new photon.PhotonImage(overPixels, overW, overH)
      const rawOversizedBase64 = Buffer.from(overImg.get_bytes()).toString("base64")
      overImg.free()

      // 2) Opaque PNG >384KB base64 -> converted to image/jpeg by Image.Service before persistence.
      const opaqueW = 640
      const opaqueH = 640
      const opaquePixels = new Uint8Array(opaqueW * opaqueH * 4)
      for (let i = 0; i < opaquePixels.length; i += 4) {
        const px = i >>> 2
        opaquePixels[i] = ((px * 73) ^ (px >>> 3)) & 0xff
        opaquePixels[i + 1] = ((px * 151) ^ (px >>> 5)) & 0xff
        opaquePixels[i + 2] = ((px * 211) ^ (px >>> 7)) & 0xff
        opaquePixels[i + 3] = 255
      }
      const opaqueImg = new photon.PhotonImage(opaquePixels, opaqueW, opaqueH)
      const rawOpaquePngBase64 = Buffer.from(opaqueImg.get_bytes()).toString("base64")
      opaqueImg.free()

      const normOversized = yield* image.normalize("huge.png", {
        uri: "huge.png",
        content: rawOversizedBase64,
        encoding: "base64",
        mime: "image/png",
      })
      const normOpaque = yield* image.normalize("photo.png", {
        uri: "photo.png",
        content: rawOpaquePngBase64,
        encoding: "base64",
        mime: "image/png",
      })
      expect(normOpaque.mime).toBe("image/jpeg")

      const populateSession = (targetSessionID: Session.ID, prefix: string) =>
        Effect.gen(function* () {
          if (targetSessionID !== sessionID) {
            yield* bus.publish(SessionEvent.Created, {
              sessionID: targetSessionID,
              slug: prefix,
              version: "test",
              projectID: Project.ID.global,
              location: { directory: AbsolutePath.make("/project") },
              subpath: RelativePath.make(""),
              title: "test",
            })
          }
          const userID = SessionMessage.ID.make(`msg_${prefix}_user`)
          const assistantID = SessionMessage.ID.make(`msg_${prefix}_assistant`)
          const queuedID = SessionMessage.ID.make(`msg_${prefix}_queued`)

          yield* inbox.admit({
            id: userID,
            sessionID: targetSessionID,
            item: {
              type: "user",
              payload: {
                text: "normalized images",
                files: [
                  {
                    data: normOversized.content,
                    mime: normOversized.mime,
                    source: { type: "inline" },
                    name: "huge.png",
                  },
                  {
                    data: normOpaque.content,
                    mime: normOpaque.mime,
                    source: { type: "uri", uri: "file:///project/photo.png" },
                    name: "photo.png",
                  },
                ],
              },
              delivery: "steer",
            },
          })
          yield* bus.publish(SessionEvent.InboxDelivered, { sessionID: targetSessionID, inboxID: userID })

          yield* bus.publish(SessionEvent.Step.Started, {
            sessionID: targetSessionID,
            assistantMessageID: assistantID,
            agent: build,
            model,
            started: 10,
          })
          yield* bus.publish(SessionEvent.Tool.Input.Started, {
            sessionID: targetSessionID,
            assistantMessageID: assistantID,
            id: "call_img",
            name: "read",
          })
          yield* bus.publish(SessionEvent.Tool.Called, {
            sessionID: targetSessionID,
            assistantMessageID: assistantID,
            id: "call_img",
            input: { path: "photo.png" },
            executed: false,
          })
          yield* bus.publish(SessionEvent.Tool.Success, {
            sessionID: targetSessionID,
            assistantMessageID: assistantID,
            id: "call_img",
            executed: false,
            content: [
              { type: "text", text: "Image read successfully" },
              {
                type: "file",
                uri: `data:${normOpaque.mime};base64,${normOpaque.content}`,
                mime: normOpaque.mime,
                name: "photo.png",
              },
            ],
          })
          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID: targetSessionID,
            assistantMessageID: assistantID,
            finish: "stop",
            cost: Money.USD.make(0),
            tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } },
          })

          yield* inbox.admit({
            id: queuedID,
            sessionID: targetSessionID,
            item: {
              type: "user",
              payload: {
                text: "queued follow-up",
                files: [
                  {
                    data: normOversized.content,
                    mime: normOversized.mime,
                    source: { type: "inline" },
                    name: "queued-huge.png",
                  },
                ],
              },
              delivery: "queue",
            },
          })
        })

      // Session 1: stored inline (experimental.mediaBlob off)
      yield* TestClock.setTime(100)
      yield* populateSession(sessionID, "compare")
      const inlineMessages = yield* store.messages({ sessionID, order: "asc" })
      const inlineInbox = yield* inbox.list(sessionID)
      const inlineExport = yield* transfer.export({ sessionID })

      expect(yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)).toHaveLength(0)

      // Clear Session 1 and run the exact same sequence with experimental.mediaBlob enabled
      yield* bus.publish(SessionEvent.Deleted, { sessionID })
      yield* db.delete(EventTable).run().pipe(Effect.orDie)
      yield* db.run(sql`DELETE FROM event_sequence`).pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "test",
          directory: "/project",
          title: "test",
          version: "test",
          time_created: 0,
          time_updated: 0,
        })
        .run()
        .pipe(Effect.orDie)
      yield* TestClock.setTime(100)
      yield* populateSession(sessionID, "compare").pipe(SessionMediaBlob.withWriteEnabled(true))

      const blobs = yield* db.select().from(MediaBlobTable).all().pipe(Effect.orDie)
      expect(blobs).toHaveLength(2)
      const byWidth = new Map(blobs.map((blob) => [blob.width, blob]))
      expect(byWidth.get(2000)?.height).toBe(1000)
      expect(byWidth.get(640)?.height).toBe(640)
      expect(byWidth.get(640)?.mime).toBe("image/jpeg")

      const refMessages = yield* store.messages({ sessionID, order: "asc" })
      const refInbox = yield* inbox.list(sessionID)
      const refExport = yield* transfer.export({ sessionID })

      expect(refMessages).toEqual(inlineMessages)
      expect(refInbox).toEqual(inlineInbox)
      expect(refExport).toEqual(inlineExport)
      expect(JSON.parse(JSON.stringify(refMessages))).toEqual(JSON.parse(JSON.stringify(inlineMessages)))
      expect(JSON.parse(JSON.stringify(refInbox))).toEqual(JSON.parse(JSON.stringify(inlineInbox)))
      expect(JSON.parse(JSON.stringify(refExport))).toEqual(JSON.parse(JSON.stringify(inlineExport)))
    }),
  )

  it.effect("replaces refs to missing blobs with an unavailable placeholder on read", () =>
    Effect.gen(function* () {
      const db = yield* seedSession()
      const inbox = yield* SessionInbox.Service
      const store = yield* SessionStore.Service
      const hash = "b".repeat(64)
      const userID = SessionMessage.ID.make("msg_missing_user")
      const assistantID = SessionMessage.ID.make("msg_missing_assistant")
      const inboxID = SessionMessage.ID.make("msg_missing_inbox")
      const placeholder = Buffer.from("[media unavailable: shot.png (image/png)]").toString("base64")

      yield* db
        .insert(SessionMessageTable)
        .values([
          {
            id: userID,
            session_id: sessionID,
            type: "user",
            seq: 1,
            time_created: 1,
            data: {
              text: "gone",
              files: [{ uri: `media:${hash}`, mime: "image/png", bytes: 10, name: "shot.png" }] as any,
              time: { created: 1 },
            },
          },
          {
            id: assistantID,
            session_id: sessionID,
            type: "assistant",
            seq: 2,
            time_created: 2,
            data: {
              agent: build,
              model,
              content: [
                {
                  type: "tool",
                  id: "call_1",
                  name: "read",
                  executed: true,
                  state: {
                    status: "completed",
                    input: { path: "shot.png" },
                    content: [{ type: "file", uri: `/api/media/${hash}`, mime: "image/png", bytes: 10 }] as any,
                  },
                  time: { created: 2, ran: 2, completed: 2 },
                },
              ],
              time: { created: 2, completed: 2 },
            },
          },
        ])
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionInboxTable)
        .values({
          id: inboxID,
          session_id: sessionID,
          type: "user",
          payload: { text: "queued", files: [{ uri: `media:${hash}`, mime: "image/png", name: "shot.png" }] as any },
          delivery: "queue",
          enqueued_seq: 3,
          time_created: 3,
        })
        .run()
        .pipe(Effect.orDie)

      const messages = yield* store.messages({ sessionID, order: "asc" })
      expect(messages[0]).toMatchObject({
        files: [{ data: placeholder, mime: "text/plain", source: { type: "inline" }, name: "shot.png" }],
      })
      expect(messages[1]).toMatchObject({
        content: [{ state: { content: [{ type: "text", text: "[media unavailable: image/png]" }] } }],
      })
      expect(yield* store.context(sessionID)).toEqual(messages)
      const listed = yield* inbox.list(sessionID)
      expect(listed[0]).toMatchObject({ payload: { files: [{ data: placeholder, mime: "text/plain" }] } })
      expect(JSON.stringify([messages, listed])).not.toContain(hash)
    }),
  )
})
