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

import { type MSC3575RoomData } from "./sliding-sync.ts";
import { type IMinimalEvent } from "./sync-accumulator.ts";
import { type Logger } from "./logger.ts";

/**
 * Persistent, per-room cache of sliding-sync room data.
 *
 * Classic /sync persists the whole sync accumulator (`store.setSyncData` →
 * `store.save()`) and rehydrates it on boot via `getSavedSync()`, so the UI paints
 * instantly from cache. Simplified sliding sync had no equivalent: rooms were only
 * ever held in memory (`store.storeRoom`), so every reload started COLD and the
 * whole view re-fetched from the network — the visible friction.
 *
 * The refoundation here is "remember the sync input, replay it on boot": we persist
 * the per-room {@link MSC3575RoomData} the server already sent us, and on startup we
 * feed each record straight back through the SAME `processRoomData` path a live
 * response takes. Equivalence to classic sync is therefore definitional — it is the
 * SDK's own ingestion of its own remembered input — and depth-tiering is automatic:
 * a bulk room cached at `timeline_limit: 1` paints its inbox row, an opened room
 * cached at `timeline_limit: 50` paints its whole timeline, with no separate code.
 *
 * It is deliberately SMALL and bounded (a curated slice, not the unbounded
 * accumulator that drove some devices over the IndexedDB quota): timelines are
 * capped, and past {@link MAX_ROOMS} the least-recently-active rooms have their
 * timelines dropped while the room itself is KEPT (see {@link shellRecord}) —
 * because the quota is spent on timelines, while it is the state that makes a
 * room visible at all. Anything goes wrong — absent IndexedDB, corruption,
 * quota — and we silently fall back to today's cold start; the cache is a pure
 * accelerator, never load-bearing.
 *
 * Kill switch: set `localStorage['mxjssdk_sss_cache_disable'] = '1'`.
 */

const DB_VERSION = 1;
/** Bump when the stored record shape changes incompatibly; mismatch wipes the store. */
const SCHEMA_VERSION = 2;
const STORE = "rooms";
const META_STORE = "meta";
/** Cap timeline events per room. Subscriptions deliver at most 50; this is a safety ceiling. */
const MAX_TIMELINE = 100;
/**
 * Cap rooms cached WITH a timeline. Past this, the least recently active are
 * SHELLED (timeline dropped, state kept) — not deleted. See {@link prune}.
 */
const MAX_ROOMS = 512;
/**
 * Hard ceiling on total records, shells included. A shell is state-only and
 * small, so this sits far above any real account; past it we genuinely delete,
 * which is the one path that can desynchronise us from a resumed pos (see
 * {@link droppedRecords}).
 */
const MAX_RECORDS = 8192;
/** Coalesce a burst of room updates into one transaction. */
const FLUSH_DEBOUNCE_MS = 1500;

export interface CachedRoomRecord {
    roomId: string;
    /** Curated MSC3575RoomData (timeline capped). */
    data: MSC3575RoomData;
    /**
     * Our own read receipt (an `m.receipt` ephemeral event) at persist time.
     * Receipts ride a SEPARATE sliding-sync extension, so they are NOT part of
     * MSC3575RoomData and would otherwise be lost across reloads — leaving the
     * rehydrated room with a stale `notification_count` and no read marker, which
     * paints an OLD unread badge until the live receipts arrive and correct it.
     * Persisting + replaying our marker makes the cached paint self-consistent.
     */
    receipt?: IMinimalEvent;
    /**
     * The room's per-room account_data at persist time (m.tag for favourites,
     * m.marked_unread, …). Like the receipt, these ride a SEPARATE sliding-sync
     * extension, so they are NOT part of MSC3575RoomData and would be lost across
     * reloads — favourites (the m.tag "pinned" state) would then revert until the
     * live account_data extension re-delivers them. Persist + replay so room.tags
     * is populated on the cached paint. (The general fix for "late sliding-sync
     * state reverts on reload": keep the last-known value until the live one lands.)
     */
    accountData?: IMinimalEvent[];
    /** Sort key for eviction + replay order (recency). */
    bump: number;
    /**
     * Never shell, never delete, replay FIRST. Set for spaces.
     *
     * A space is pure structure: no timeline (the spaces list runs
     * `timeline_limit: 0`), so nothing ever bumps it and recency-ordered
     * eviction sorts it straight to the front of the queue — the cache would
     * discard the sidebar to keep the chatter. They are also few and tiny, so
     * pinning them costs nothing. Kept separate from `bump` so that field keeps
     * meaning "recency" and nothing else.
     */
    pin?: 1;
    /** Wall-clock of last write, for tie-breaking / diagnostics. */
    ts: number;
    schema: number;
}

/** Does this room's cached state say it is a space? */
export function isSpaceData(data: MSC3575RoomData): boolean {
    return (data.required_state ?? []).some(
        (e) =>
            e.type === "m.room.create" &&
            e.state_key === "" &&
            (e.content as { type?: string } | undefined)?.type === "m.space",
    );
}

/**
 * Strip a record to a SHELL: room identity and state, no timeline.
 *
 * The cache is capped because IndexedDB quota is finite — but the quota is
 * consumed almost entirely by TIMELINES (up to {@link MAX_TIMELINE} events per
 * room), while what makes a room *known* is its state: a handful of events.
 * Deleting whole records to protect the quota therefore threw away the cheap,
 * load-bearing part to save the expensive part — and a deleted record under a
 * resumed pos is an INVISIBLE room, because the server only ever sends deltas
 * for rooms it believes we hold.
 *
 * So: drop the timeline, keep the room. A shelled room rehydrates as itself —
 * right name, avatar, membership, space-ness, tombstone — with an empty
 * timeline that the live sync (list `timeline_limit: 1`, or the room's own
 * subscription at 50 when opened) fills in. A known room with no preview beats
 * a room that isn't there.
 *
 * `prev_batch` goes with the timeline deliberately. It is the token for the
 * START of the window we're dropping, so keeping it would let back-pagination
 * resume BEHIND the events we just discarded and silently skip them — the same
 * mid-timeline hole the merge path avoids. With no token and an empty timeline
 * the room simply paginates from scratch once it has live events again.
 */
export function shellRecord(rec: CachedRoomRecord): CachedRoomRecord {
    const { prev_batch: _dropped, ...rest } = rec.data;
    return {
        ...rec,
        data: { ...rest, timeline: [], limited: true } as MSC3575RoomData,
    };
}

const isShell = (rec: CachedRoomRecord): boolean => (rec.data.timeline?.length ?? 0) === 0;

/**
 * Decide what to shell and what to delete. Pure; exported for tests.
 * See {@link SlidingSyncCache.prune} for the reasoning behind the two caps.
 */
export function planPrune(records: CachedRoomRecord[]): { shell: string[]; drop: string[] } {
    const evictable = records.filter((r) => !r.pin);
    const byBumpAscending = (a: CachedRoomRecord, b: CachedRoomRecord): number => a.bump - b.bump;

    const full = evictable.filter((r) => !isShell(r)).sort(byBumpAscending);
    const shell = full.slice(0, Math.max(0, full.length - MAX_ROOMS)).map((r) => r.roomId);

    // Shelling frees no RECORDS, so the total cap is measured against the whole
    // set and satisfied only by deleting. Records shelled above are already
    // counted here as the shells they are about to become.
    const shellSet = new Set(shell);
    const drop =
        records.length > MAX_RECORDS
            ? evictable
                  .filter((r) => isShell(r) || shellSet.has(r.roomId))
                  .sort(byBumpAscending)
                  .slice(0, records.length - MAX_RECORDS)
                  .map((r) => r.roomId)
            : [];

    const dropSet = new Set(drop);
    return { shell: shell.filter((id) => !dropSet.has(id)), drop };
}

function cacheDisabled(): boolean {
    try {
        return globalThis.localStorage?.getItem("mxjssdk_sss_cache_disable") === "1";
    } catch {
        return false;
    }
}

/** Strip a room's data down to the bounded slice we persist. */
function curate(data: MSC3575RoomData): MSC3575RoomData {
    const timeline = data.timeline ?? [];
    return {
        ...data,
        // Keep the most RECENT events (the tail); back-pagination uses prev_batch.
        timeline: timeline.length > MAX_TIMELINE ? timeline.slice(timeline.length - MAX_TIMELINE) : timeline,
    };
}

type TimelineEntry = MSC3575RoomData["timeline"][number];
const eventIdOf = (e: TimelineEntry): string | undefined => (e as { event_id?: string }).event_id;

/**
 * Merge a room's incoming sliding-sync data into the previously cached record.
 * Exported for tests.
 *
 * Under a stateful connection (conn_id) the server sends sparse DELTAS: a
 * response for a known room carries only the new timeline events and only the
 * changed state, and omits fields that didn't change. Persisting such a delta
 * wholesale (the old behaviour) meant the last delta before shutdown BECAME the
 * entire cached record — the next boot painted the room as one event with
 * near-empty state. So we merge instead, per THE ONE RULE's "keep last-known":
 *
 * - scalar fields: incoming non-null wins, otherwise keep the cached value;
 * - required_state: union by (type, state_key), incoming wins;
 * - timeline: `initial` replaces everything (server re-sent from scratch);
 *   `limited` replaces the timeline + prev_batch (a limited delta does NOT
 *   connect to our cached tail — appending would bake an invisible gap into
 *   the cached record); otherwise the delta is contiguous with our tail, so
 *   append (deduped) and KEEP the cached prev_batch, which still matches the
 *   timeline's start. If a contiguous append would overflow {@link MAX_TIMELINE},
 *   fall back to just the delta window with ITS prev_batch — trimming the head
 *   of a merged timeline instead would leave prev_batch pointing BEFORE the
 *   trimmed events, and back-pagination after reload would silently skip them
 *   (a permanent mid-timeline hole). A thin-but-correct paint beats that.
 */
export function mergeRoomData(prev: MSC3575RoomData | undefined, next: MSC3575RoomData): MSC3575RoomData {
    if (!prev || next.initial) {
        return curate(next);
    }
    // Keep-last-known scalars: only let DEFINED incoming fields overwrite.
    const overlay: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(next)) {
        if (v !== undefined && v !== null) overlay[k] = v;
    }
    const merged = { ...prev, ...overlay } as MSC3575RoomData;

    // required_state: union by (type, state_key), incoming wins.
    const state = new Map<string, TimelineEntry>();
    for (const ev of prev.required_state ?? []) state.set(`${ev.type}|${ev.state_key}`, ev);
    for (const ev of next.required_state ?? []) state.set(`${ev.type}|${ev.state_key}`, ev);
    merged.required_state = [...state.values()] as MSC3575RoomData["required_state"];

    const prevTimeline = prev.timeline ?? [];
    const nextTimeline = next.timeline ?? [];
    if (next.limited) {
        // Doesn't connect to our cached tail: replace. prev_batch (if any)
        // matches the new window's start; an absent one wipes the old token,
        // which no longer describes this timeline's start either.
        merged.timeline = nextTimeline;
        merged.prev_batch = next.prev_batch;
    } else {
        const seen = new Set(prevTimeline.map(eventIdOf));
        const appended = [...prevTimeline, ...nextTimeline.filter((e) => !eventIdOf(e) || !seen.has(eventIdOf(e)))];
        if (appended.length > MAX_TIMELINE && nextTimeline.length > 0 && next.prev_batch) {
            merged.timeline = nextTimeline;
            merged.prev_batch = next.prev_batch;
        } else {
            merged.timeline = appended;
            merged.prev_batch = prev.prev_batch;
        }
    }
    return curate(merged);
}

export class SlidingSyncCache {
    private dbPromise: Promise<IDBDatabase | null> | null = null;
    /**
     * Session store-of-record: the MERGED cached record per room, seeded from
     * IndexedDB by {@link loadAll} and updated by every {@link put}. Deltas
     * merge against this synchronously (no IDB read on the put path); flush
     * writes the dirty subset out. Memory is bounded by the account's room
     * count, same order as the SDK's own Room store.
     */
    private records = new Map<string, CachedRoomRecord>();
    private dirty = new Set<string>();
    /** Records to delete from IDB on the next flush (see {@link prune}). */
    private deletes = new Set<string>();
    /** Set when a record was genuinely deleted — this session or a previous one. */
    private dropped = false;
    private flushTimer: ReturnType<typeof setTimeout> | null = null;
    private closed = false;
    private readonly dbName: string;

    public constructor(
        userId: string | undefined,
        private readonly logger: Logger,
    ) {
        this.dbName = `mxjssdk_sss_cache_${userId ?? "anon"}`;
    }

    private get idb(): IDBFactory | undefined {
        try {
            return globalThis.indexedDB;
        } catch {
            return undefined;
        }
    }

    private open(): Promise<IDBDatabase | null> {
        if (this.dbPromise) return this.dbPromise;
        this.dbPromise = new Promise<IDBDatabase | null>((resolve) => {
            const idb = this.idb;
            if (!idb || cacheDisabled()) {
                resolve(null);
                return;
            }
            let req: IDBOpenDBRequest;
            try {
                req = idb.open(this.dbName, DB_VERSION);
            } catch (e) {
                this.logger.warn("[sss-cache] open threw; disabling cache", e);
                resolve(null);
                return;
            }
            req.onupgradeneeded = (): void => {
                const db = req.result;
                if (!db.objectStoreNames.contains(STORE)) {
                    const os = db.createObjectStore(STORE, { keyPath: "roomId" });
                    os.createIndex("bump", "bump");
                }
                if (!db.objectStoreNames.contains(META_STORE)) {
                    db.createObjectStore(META_STORE);
                }
            };
            req.onsuccess = (): void => resolve(req.result);
            req.onerror = (): void => {
                this.logger.warn("[sss-cache] open failed; disabling cache", req.error);
                resolve(null);
            };
            req.onblocked = (): void => resolve(null);
        });
        return this.dbPromise;
    }

    private tx(db: IDBDatabase, stores: string[], mode: IDBTransactionMode): IDBTransaction {
        return db.transaction(stores, mode);
    }

    /**
     * Load all cached rooms for replay, newest-active first, after validating the
     * schema version (a mismatch wipes the store and returns nothing). Returns the
     * curated {@link MSC3575RoomData} with `initial`/`limited` forced so the caller
     * can feed it straight through the live ingestion path.
     */
    public async loadAll(): Promise<
        { roomId: string; data: MSC3575RoomData; receipt?: IMinimalEvent; accountData?: IMinimalEvent[] }[]
    > {
        const db = await this.open();
        if (!db) return [];
        try {
            const okSchema = await new Promise<boolean>((resolve) => {
                const r = this.tx(db, [META_STORE], "readonly").objectStore(META_STORE).get("schema");
                r.onsuccess = (): void => resolve((r.result ?? SCHEMA_VERSION) === SCHEMA_VERSION);
                r.onerror = (): void => resolve(false);
            });
            if (!okSchema) {
                this.logger.info("[sss-cache] schema changed; clearing cache");
                await this.clear();
                return [];
            }
            // Did a previous session genuinely DELETE any record? If so the
            // persisted pos is no longer safe to resume — see droppedRecords.
            this.dropped = await new Promise<boolean>((resolve) => {
                const r = this.tx(db, [META_STORE], "readonly").objectStore(META_STORE).get("dropped");
                r.onsuccess = (): void => resolve(r.result === true);
                r.onerror = (): void => resolve(false);
            });
            const records = await new Promise<CachedRoomRecord[]>((resolve) => {
                const out: CachedRoomRecord[] = [];
                const idx = this.tx(db, [STORE], "readonly").objectStore(STORE).index("bump");
                // Descending by bump → most recently active rooms replay first.
                const cursorReq = idx.openCursor(null, "prev");
                cursorReq.onsuccess = (): void => {
                    const cur = cursorReq.result;
                    if (!cur) {
                        resolve(out);
                        return;
                    }
                    out.push(cur.value as CachedRoomRecord);
                    cur.continue();
                };
                cursorReq.onerror = (): void => resolve(out);
            });
            const valid = records.filter((r) => r && r.schema === SCHEMA_VERSION && r.data && r.roomId);
            // Pinned rooms (spaces) replay FIRST, then the rest by recency. The
            // sidebar's structure is what the whole UI hangs off, so paint it
            // before the chatter rather than somewhere in the middle of 500
            // rooms. Stable within each group (the cursor already ordered by
            // bump descending).
            valid.sort((a, b) => (b.pin ?? 0) - (a.pin ?? 0));
            // Seed the merge base: subsequent live DELTAS for these rooms merge
            // against what we just loaded rather than replacing it.
            for (const r of valid) {
                if (!this.records.has(r.roomId)) this.records.set(r.roomId, r);
            }
            return valid.map((r) => ({
                roomId: r.roomId,
                data: { ...r.data, initial: true, limited: true },
                receipt: r.receipt,
                accountData: r.accountData,
            }));
        } catch (e) {
            this.logger.warn("[sss-cache] loadAll failed", e);
            return [];
        }
    }

    /**
     * Merge a room's data into the session record (deltas merge, initial
     * replaces — see {@link mergeRoomData}) and queue it for persistence
     * (debounced + coalesced).
     */
    public put(
        roomId: string,
        data: MSC3575RoomData,
        receipt?: IMinimalEvent,
        accountData?: IMinimalEvent[],
    ): void {
        if (this.closed || !this.idb || cacheDisabled()) return;
        try {
            const prev = this.records.get(roomId);
            const merged = mergeRoomData(prev?.data, data);
            this.records.set(roomId, {
                roomId,
                data: merged,
                // receipt/accountData are computed fresh from the Room (already
                // merged truth) on every put, but keep the last-known copy when
                // a put omits them.
                receipt: receipt ?? prev?.receipt,
                accountData: accountData ?? prev?.accountData,
                bump: data.bump_stamp ?? Date.now(),
                // Re-evaluated per put against the MERGED state: a room whose
                // create event only arrives on a later delta still becomes
                // pinned, and one that is not a space never does.
                ...(isSpaceData(merged) ? { pin: 1 as const } : {}),
                ts: Date.now(),
                schema: SCHEMA_VERSION,
            });
            this.dirty.add(roomId);
        } catch {
            return;
        }
        if (!this.flushTimer) {
            this.flushTimer = setTimeout(() => void this.flush(), FLUSH_DEBOUNCE_MS);
        }
    }

    /**
     * Was a record genuinely deleted (not shelled), here or in a past session?
     *
     * THE POS COUPLING. Under a stateful connection the server sends deltas for
     * rooms it believes we hold, so resuming a pos is only sound while the cache
     * still holds everything it held when that pos was written. Shelling keeps
     * the room, so it preserves the coupling; deleting breaks it, and the room
     * is then invisible until something forces a reinitialise. The caller drops
     * the pos when this is true, trading one full resync for correctness.
     *
     * Cleared by {@link clearDropped} once the caller has acted on it.
     */
    public get droppedRecords(): boolean {
        return this.dropped;
    }

    /** Acknowledge {@link droppedRecords}: the caller has dropped the pos. */
    public async clearDropped(): Promise<void> {
        this.dropped = false;
        const db = await this.open();
        if (!db) return;
        await new Promise<void>((resolve) => {
            try {
                const t = this.tx(db, [META_STORE], "readwrite");
                t.objectStore(META_STORE).delete("dropped");
                t.oncomplete = (): void => resolve();
                t.onerror = (): void => resolve();
                t.onabort = (): void => resolve();
            } catch {
                resolve();
            }
        });
    }

    /**
     * Bring the record set back within its caps, in memory, before the write.
     *
     * Two caps, two very different remedies:
     *  - more than {@link MAX_ROOMS} rooms carrying a timeline → SHELL the least
     *    recently active (see {@link shellRecord}). The room stays known; only
     *    its timeline goes. This is the normal, routine case and it costs
     *    nothing but a missing preview until the next live delivery.
     *  - more than {@link MAX_RECORDS} records in total → genuinely delete the
     *    least recently active shells, and flag it (see {@link droppedRecords}).
     *    Far above any real account; the flag exists so that if it ever does
     *    happen we resync instead of silently losing rooms.
     *
     * Pinned records (spaces) are exempt from both.
     *
     * Runs against `this.records`, which loadAll seeds with every stored record,
     * so it is the authoritative view — no cursor walk, and shelled records
     * simply stop counting toward MAX_ROOMS instead of being re-examined on
     * every flush.
     */
    private prune(): void {
        const { shell, drop } = planPrune([...this.records.values()]);
        for (const roomId of shell) {
            const rec = this.records.get(roomId);
            if (!rec) continue;
            this.records.set(roomId, shellRecord(rec));
            this.dirty.add(roomId);
        }
        for (const roomId of drop) {
            this.records.delete(roomId);
            this.dirty.delete(roomId);
            this.deletes.add(roomId);
            this.dropped = true;
        }
    }

    /** Prune to the caps, then write everything pending in one transaction. */
    public async flush(): Promise<void> {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        if (this.dirty.size === 0 && this.deletes.size === 0) return;
        this.prune();
        const batch = [...this.dirty].map((roomId) => this.records.get(roomId)).filter((r): r is CachedRoomRecord => !!r);
        const deletes = [...this.deletes];
        const dropped = this.dropped;
        this.dirty.clear();
        this.deletes.clear();
        const db = await this.open();
        if (!db) return;
        try {
            await new Promise<void>((resolve) => {
                const t = this.tx(db, [STORE, META_STORE], "readwrite");
                const os = t.objectStore(STORE);
                for (const rec of batch) os.put(rec);
                for (const roomId of deletes) os.delete(roomId);
                t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
                // Same transaction as the deletes: the flag can never be lost
                // while the deletion it describes survives.
                if (dropped) t.objectStore(META_STORE).put(true, "dropped");
                t.oncomplete = (): void => resolve();
                t.onerror = (): void => resolve();
                t.onabort = (): void => resolve();
            });
        } catch (e) {
            this.logger.warn("[sss-cache] flush failed", e);
        }
    }

    /** Drop the entire cache (used on schema change / reset). */
    public async clear(): Promise<void> {
        this.records.clear();
        this.dirty.clear();
        this.deletes.clear();
        // A cleared cache restores nothing, so the caller drops the pos on the
        // `replayed === 0` path anyway; leaving the flag set would force a
        // second, pointless resync on the boot after that.
        this.dropped = false;
        const db = await this.open();
        if (!db) return;
        await new Promise<void>((resolve) => {
            try {
                const t = this.tx(db, [STORE, META_STORE], "readwrite");
                t.objectStore(STORE).clear();
                t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
                t.objectStore(META_STORE).delete("dropped");
                t.oncomplete = (): void => resolve();
                t.onerror = (): void => resolve();
                t.onabort = (): void => resolve();
            } catch {
                resolve();
            }
        });
    }

    /** Flush any pending writes and stop accepting new ones. */
    public async stop(): Promise<void> {
        this.closed = true;
        await this.flush();
    }
}
