/*
Copyright 2026 The Matrix.org Foundation C.I.C.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import {
    type CachedRoomRecord,
    isSpaceData,
    mergeRoomData,
    planPrune,
    repairUnfillable,
    shellRecord,
} from "../../src/sliding-sync-cache";
import { type MSC3575RoomData } from "../../src/sliding-sync";
import { type IRoomEvent, type IStateEvent } from "../../src";

describe("SlidingSyncCache mergeRoomData", () => {
    let counter = 0;
    const mkEvent = (body: string): IRoomEvent => ({
        type: "m.room.message",
        content: { body },
        sender: "@alice:localhost",
        origin_server_ts: ++counter,
        event_id: `$${counter}`,
    });
    const mkState = (type: string, stateKey: string, content: object = {}): IStateEvent => ({
        type,
        state_key: stateKey,
        content,
        sender: "@alice:localhost",
        origin_server_ts: ++counter,
        event_id: `$${counter}`,
    });
    const mkData = (overrides: Partial<MSC3575RoomData> = {}): MSC3575RoomData => ({
        name: "Room",
        required_state: [],
        timeline: [],
        ...overrides,
    });

    it("replaces everything when there is no previous record", () => {
        const next = mkData({ timeline: [mkEvent("a")], prev_batch: "tok" });
        expect(mergeRoomData(undefined, next)).toEqual(next);
    });

    it("replaces everything on an initial response", () => {
        const prev = mkData({
            timeline: [mkEvent("old")],
            required_state: [mkState("m.room.topic", "")],
            prev_batch: "old-tok",
        });
        const next = mkData({ timeline: [mkEvent("fresh")], initial: true, prev_batch: "fresh-tok" });
        const merged = mergeRoomData(prev, next);
        expect(merged.timeline).toEqual(next.timeline);
        expect(merged.prev_batch).toEqual("fresh-tok");
        // initial REPLACES state too — the server re-sent from scratch
        expect(merged.required_state).toEqual([]);
    });

    it("keeps last-known scalar fields a delta omits", () => {
        const prev = mkData({ name: "Named", notification_count: 3, joined_count: 7, is_dm: true });
        const next = mkData({ timeline: [mkEvent("d")], notification_count: 0 });
        delete (next as Partial<MSC3575RoomData>).name; // deltas can omit name
        const merged = mergeRoomData(prev, next);
        expect(merged.name).toEqual("Named"); // kept
        expect(merged.notification_count).toEqual(0); // 0 is a real value, not an omission
        expect(merged.joined_count).toEqual(7); // kept
        expect(merged.is_dm).toEqual(true); // kept
    });

    it("unions required_state by (type, state_key) with the delta winning", () => {
        const prev = mkData({
            required_state: [mkState("m.room.name", "", { name: "old" }), mkState("m.room.encryption", "")],
        });
        const newName = mkState("m.room.name", "", { name: "new" });
        const merged = mergeRoomData(prev, mkData({ required_state: [newName] }));
        expect(merged.required_state).toHaveLength(2);
        expect(merged.required_state.find((e) => e.type === "m.room.name")!.content).toEqual({ name: "new" });
        expect(merged.required_state.find((e) => e.type === "m.room.encryption")).toBeTruthy();
    });

    it("appends a contiguous (non-limited) delta and keeps the cached prev_batch", () => {
        const a = mkEvent("a");
        const b = mkEvent("b");
        const c = mkEvent("c");
        const prev = mkData({ timeline: [a, b], prev_batch: "deep-history" });
        // deltas re-cover the previous window (lag-one ack), so b comes again
        const merged = mergeRoomData(prev, mkData({ timeline: [b, c], prev_batch: "mid-window" }));
        expect(merged.timeline).toEqual([a, b, c]); // deduped append
        expect(merged.prev_batch).toEqual("deep-history"); // still matches the timeline start
    });

    it("replaces the timeline AND prev_batch on a limited delta (gap to the cached tail)", () => {
        const prev = mkData({ timeline: [mkEvent("old")], prev_batch: "deep-history" });
        const burst = [mkEvent("burst1"), mkEvent("burst2")];
        const merged = mergeRoomData(prev, mkData({ timeline: burst, limited: true, prev_batch: "burst-tok" }));
        expect(merged.timeline).toEqual(burst); // appending would bake in an invisible gap
        expect(merged.prev_batch).toEqual("burst-tok");
    });

    it("falls back to the delta window when an append would overflow the cap", () => {
        // Trimming the head of a merged timeline would leave prev_batch pointing
        // BEFORE the trimmed events — back-pagination after reload would skip
        // them silently. A thin-but-correct paint beats a permanent mid-hole.
        const prevTimeline = Array.from({ length: 100 }, (_, i) => mkEvent(`old${i}`));
        const prev = mkData({ timeline: prevTimeline, prev_batch: "deep-history" });
        const fresh = [mkEvent("fresh1"), mkEvent("fresh2")];
        const merged = mergeRoomData(prev, mkData({ timeline: fresh, prev_batch: "fresh-tok" }));
        expect(merged.timeline).toEqual(fresh);
        expect(merged.prev_batch).toEqual("fresh-tok");
    });
});

describe("SlidingSyncCache planPrune", () => {
    const MAX_ROOMS = 512;
    const MAX_RECORDS = 8192;

    const mkRec = (
        roomId: string,
        { bump = 1, events = 1, pin = false }: { bump?: number; events?: number; pin?: boolean } = {},
    ): CachedRoomRecord =>
        ({
            roomId,
            data: {
                name: roomId,
                required_state: [],
                timeline: Array.from({ length: events }, (_, i) => ({
                    type: "m.room.message",
                    content: { body: `e${i}` },
                    sender: "@alice:localhost",
                    origin_server_ts: i,
                    event_id: `$${roomId}-${i}`,
                })),
            },
            bump,
            ts: 0,
            schema: 2,
            ...(pin ? { pin: 1 as const } : {}),
        }) as CachedRoomRecord;

    const mkMany = (n: number, opts?: Parameters<typeof mkRec>[1]): CachedRoomRecord[] =>
        Array.from({ length: n }, (_, i) => mkRec(`!r${i}`, { bump: i + 1, ...opts }));

    it("does nothing while under both caps", () => {
        expect(planPrune(mkMany(MAX_ROOMS))).toEqual({ shell: [], drop: [] });
    });

    it("shells the least recently active rooms past the room cap, and never deletes them", () => {
        const plan = planPrune(mkMany(MAX_ROOMS + 3));
        // bump ascending → the three oldest
        expect(plan.shell).toEqual(["!r0", "!r1", "!r2"]);
        expect(plan.drop).toEqual([]);
    });

    it("exempts pinned records (spaces) from shelling, even as the quietest rooms", () => {
        const records = [
            ...mkMany(MAX_ROOMS, { bump: 1000 }),
            mkRec("!space", { bump: 0, events: 0, pin: true }),
            mkRec("!quiet", { bump: 1 }),
        ];
        const plan = planPrune(records);
        expect(plan.shell).toContain("!quiet");
        expect(plan.shell).not.toContain("!space");
        expect(plan.drop).not.toContain("!space");
    });

    it("does not re-shell records that are already shells", () => {
        // Every room over the cap is already a shell → nothing left to free.
        const records = [...mkMany(MAX_ROOMS, { events: 1 }), ...mkMany(50, { events: 0 })];
        expect(planPrune(records).shell).toEqual([]);
    });

    it("deletes only past the hard record cap, oldest shells first", () => {
        const records = [
            ...mkMany(MAX_ROOMS, { bump: 10_000, events: 1 }),
            ...Array.from({ length: MAX_RECORDS - MAX_ROOMS + 2 }, (_, i) =>
                mkRec(`!s${i}`, { bump: i + 1, events: 0 }),
            ),
        ];
        const plan = planPrune(records);
        expect(plan.drop).toEqual(["!s0", "!s1"]);
    });

    it("never both shells and deletes the same record", () => {
        const records = [
            ...mkMany(MAX_ROOMS + 1, { bump: 10_000, events: 1 }),
            ...Array.from({ length: MAX_RECORDS - MAX_ROOMS }, (_, i) => mkRec(`!s${i}`, { bump: 5, events: 0 })),
        ];
        const plan = planPrune(records);
        expect(plan.shell.filter((id) => plan.drop.includes(id))).toEqual([]);
    });
});

describe("SlidingSyncCache shellRecord / isSpaceData", () => {
    const rec = {
        roomId: "!r",
        data: {
            name: "Room",
            required_state: [{ type: "m.room.create", state_key: "", content: {} }],
            timeline: [{ type: "m.room.message", content: { body: "hi" }, event_id: "$1" }],
            prev_batch: "tok",
            bump_stamp: 7,
        },
        bump: 7,
        ts: 0,
        schema: 2,
    } as unknown as CachedRoomRecord;

    it("keeps identity and state, drops the timeline", () => {
        const shelled = shellRecord(rec);
        expect(shelled.roomId).toEqual("!r");
        expect(shelled.data.name).toEqual("Room");
        expect(shelled.data.required_state).toEqual(rec.data.required_state);
        expect(shelled.data.timeline).toEqual([]);
        expect(shelled.bump).toEqual(7);
    });

    it("drops prev_batch with the timeline, so back-pagination can't skip the discarded events", () => {
        expect(shellRecord(rec).data.prev_batch).toBeUndefined();
    });

    it("marks the shell limited, so a live delta replaces rather than appends to nothing", () => {
        expect(shellRecord(rec).data.limited).toBe(true);
    });

    it("detects a space from its create event, and only a space", () => {
        const mk = (content: object): MSC3575RoomData =>
            ({
                required_state: [{ type: "m.room.create", state_key: "", content }],
                timeline: [],
            }) as unknown as MSC3575RoomData;
        expect(isSpaceData(mk({ type: "m.space" }))).toBe(true);
        expect(isSpaceData(mk({}))).toBe(false);
        expect(isSpaceData({ required_state: [], timeline: [] } as unknown as MSC3575RoomData)).toBe(false);
    });
});

describe("SlidingSyncCache mergeRoomData onto a shell", () => {
    let counter = 0;
    const mkEvent = (body: string): IRoomEvent => ({
        type: "m.room.message",
        content: { body },
        sender: "@alice:localhost",
        origin_server_ts: ++counter,
        event_id: `$shell${counter}`,
    });
    const mkData = (overrides: Partial<MSC3575RoomData> = {}): MSC3575RoomData =>
        ({ name: "Room", required_state: [], timeline: [], ...overrides }) as MSC3575RoomData;

    it("takes the delta's prev_batch when the cached timeline is empty", () => {
        // A shelled record: state kept, timeline and token dropped. The delta that
        // follows is an ordinary non-limited one — it is NOT contiguous with
        // anything, because there is nothing there.
        const shell = mkData({ required_state: [], timeline: [], limited: true });
        const delta = mkData({ timeline: [mkEvent("bridged message")], prev_batch: "tok-from-delta" });

        const merged = mergeRoomData(shell, delta);

        expect(merged.timeline).toEqual(delta.timeline);
        // Without this the record caches events with NO token, and the room comes
        // back after a reload showing only those events, unable to paginate.
        expect(merged.prev_batch).toEqual("tok-from-delta");
    });

    it("still keeps the cached token when there IS a tail to be contiguous with", () => {
        const cached = mkData({ timeline: [mkEvent("old")], prev_batch: "tok-cached" });
        const delta = mkData({ timeline: [mkEvent("new")], prev_batch: "tok-from-delta" });

        const merged = mergeRoomData(cached, delta);

        expect(merged.timeline).toHaveLength(2);
        expect(merged.prev_batch).toEqual("tok-cached");
    });

    it("keeps the room's state across the shell → delta merge", () => {
        const shell = mkData({
            required_state: [
                {
                    type: "m.room.create",
                    state_key: "",
                    content: { type: "m.space" },
                    sender: "@a:b",
                    origin_server_ts: 1,
                    event_id: "$c",
                } as unknown as IStateEvent,
            ],
            timeline: [],
        });
        const merged = mergeRoomData(shell, mkData({ timeline: [mkEvent("hi")], prev_batch: "t" }));
        expect(merged.required_state).toHaveLength(1);
    });
});

describe("SlidingSyncCache repairUnfillable", () => {
    const rec = (data: Partial<MSC3575RoomData>): CachedRoomRecord =>
        ({
            roomId: "!r",
            data: { name: "R", required_state: [], timeline: [], ...data },
            bump: 1,
            ts: 0,
            schema: 2,
        }) as CachedRoomRecord;
    const ev = { type: "m.room.message", content: {}, event_id: "$1" } as unknown as IRoomEvent;

    it("drops a timeline that has no pagination token", () => {
        const out = repairUnfillable(rec({ timeline: [ev] }));
        expect(out.data.timeline).toEqual([]);
        expect(out.data.limited).toBe(true);
    });

    it("leaves a timeline that HAS a token alone", () => {
        const input = rec({ timeline: [ev], prev_batch: "tok" });
        expect(repairUnfillable(input)).toBe(input);
    });

    it("leaves an already-shelled record alone", () => {
        const input = rec({ timeline: [] });
        expect(repairUnfillable(input)).toBe(input);
    });
});
