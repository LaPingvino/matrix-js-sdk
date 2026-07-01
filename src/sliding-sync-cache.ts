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
 * capped and the least-recently-active rooms are evicted. Anything goes wrong —
 * absent IndexedDB, corruption, quota — and we silently fall back to today's cold
 * start; the cache is a pure accelerator, never load-bearing.
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
/** Cap total cached rooms; the lowest-bump (least recently active) are evicted past this. */
const MAX_ROOMS = 512;
/** Coalesce a burst of room updates into one transaction. */
const FLUSH_DEBOUNCE_MS = 1500;

interface CachedRoomRecord {
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
    /** Wall-clock of last write, for tie-breaking / diagnostics. */
    ts: number;
    schema: number;
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
            this.records.set(roomId, {
                roomId,
                data: mergeRoomData(prev?.data, data),
                // receipt/accountData are computed fresh from the Room (already
                // merged truth) on every put, but keep the last-known copy when
                // a put omits them.
                receipt: receipt ?? prev?.receipt,
                accountData: accountData ?? prev?.accountData,
                bump: data.bump_stamp ?? Date.now(),
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

    /** Write all dirty records, then evict down to MAX_ROOMS by lowest bump. */
    public async flush(): Promise<void> {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        if (this.dirty.size === 0) return;
        const batch = [...this.dirty].map((roomId) => this.records.get(roomId)).filter((r): r is CachedRoomRecord => !!r);
        this.dirty.clear();
        const db = await this.open();
        if (!db) return;
        try {
            await new Promise<void>((resolve) => {
                const t = this.tx(db, [STORE, META_STORE], "readwrite");
                const os = t.objectStore(STORE);
                for (const rec of batch) os.put(rec);
                t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
                t.oncomplete = (): void => resolve();
                t.onerror = (): void => resolve();
                t.onabort = (): void => resolve();
            });
            await this.evict(db);
        } catch (e) {
            this.logger.warn("[sss-cache] flush failed", e);
        }
    }

    private async evict(db: IDBDatabase): Promise<void> {
        try {
            const count = await new Promise<number>((resolve) => {
                const r = this.tx(db, [STORE], "readonly").objectStore(STORE).count();
                r.onsuccess = (): void => resolve(r.result);
                r.onerror = (): void => resolve(0);
            });
            if (count <= MAX_ROOMS) return;
            const toDrop = count - MAX_ROOMS;
            await new Promise<void>((resolve) => {
                const t = this.tx(db, [STORE], "readwrite");
                // Ascending by bump → drop the least recently active first.
                const cursorReq = t.objectStore(STORE).index("bump").openCursor(null, "next");
                let dropped = 0;
                cursorReq.onsuccess = (): void => {
                    const cur = cursorReq.result;
                    if (!cur || dropped >= toDrop) {
                        resolve();
                        return;
                    }
                    cur.delete();
                    dropped++;
                    cur.continue();
                };
                cursorReq.onerror = (): void => resolve();
                t.onabort = (): void => resolve();
            });
        } catch (e) {
            this.logger.warn("[sss-cache] evict failed", e);
        }
    }

    /** Drop the entire cache (used on schema change / reset). */
    public async clear(): Promise<void> {
        this.records.clear();
        this.dirty.clear();
        const db = await this.open();
        if (!db) return;
        await new Promise<void>((resolve) => {
            try {
                const t = this.tx(db, [STORE, META_STORE], "readwrite");
                t.objectStore(STORE).clear();
                t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
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
