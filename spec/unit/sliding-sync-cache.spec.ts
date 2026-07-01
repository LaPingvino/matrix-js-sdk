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

import { mergeRoomData } from "../../src/sliding-sync-cache";
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
