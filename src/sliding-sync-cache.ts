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

export class SlidingSyncCache {
    private dbPromise: Promise<IDBDatabase | null> | null = null;
    private pending = new Map<string, CachedRoomRecord>();
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
    public async loadAll(): Promise<{ roomId: string; data: MSC3575RoomData; receipt?: IMinimalEvent }[]> {
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
            return records
                .filter((r) => r && r.schema === SCHEMA_VERSION && r.data && r.roomId)
                .map((r) => ({ roomId: r.roomId, data: { ...r.data, initial: true, limited: true }, receipt: r.receipt }));
        } catch (e) {
            this.logger.warn("[sss-cache] loadAll failed", e);
            return [];
        }
    }

    /** Queue a room's data for persistence (debounced + coalesced). */
    public put(roomId: string, data: MSC3575RoomData, receipt?: IMinimalEvent): void {
        if (this.closed || !this.idb || cacheDisabled()) return;
        try {
            this.pending.set(roomId, {
                roomId,
                data: curate(data),
                receipt,
                bump: data.bump_stamp ?? Date.now(),
                ts: Date.now(),
                schema: SCHEMA_VERSION,
            });
        } catch {
            return;
        }
        if (!this.flushTimer) {
            this.flushTimer = setTimeout(() => void this.flush(), FLUSH_DEBOUNCE_MS);
        }
    }

    /** Write all pending records, then evict down to MAX_ROOMS by lowest bump. */
    public async flush(): Promise<void> {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        if (this.pending.size === 0) return;
        const batch = [...this.pending.values()];
        this.pending.clear();
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
