import { type MSC3575RoomData } from "./sliding-sync.ts";
import { type IMinimalEvent } from "./sync-accumulator.ts";
import { type Logger } from "./logger.ts";
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
export declare function mergeRoomData(prev: MSC3575RoomData | undefined, next: MSC3575RoomData): MSC3575RoomData;
export declare class SlidingSyncCache {
    private readonly logger;
    private dbPromise;
    /**
     * Session store-of-record: the MERGED cached record per room, seeded from
     * IndexedDB by {@link loadAll} and updated by every {@link put}. Deltas
     * merge against this synchronously (no IDB read on the put path); flush
     * writes the dirty subset out. Memory is bounded by the account's room
     * count, same order as the SDK's own Room store.
     */
    private records;
    private dirty;
    private flushTimer;
    private closed;
    private readonly dbName;
    constructor(userId: string | undefined, logger: Logger);
    private get idb();
    private open;
    private tx;
    /**
     * Load all cached rooms for replay, newest-active first, after validating the
     * schema version (a mismatch wipes the store and returns nothing). Returns the
     * curated {@link MSC3575RoomData} with `initial`/`limited` forced so the caller
     * can feed it straight through the live ingestion path.
     */
    loadAll(): Promise<{
        roomId: string;
        data: MSC3575RoomData;
        receipt?: IMinimalEvent;
        accountData?: IMinimalEvent[];
    }[]>;
    /**
     * Merge a room's data into the session record (deltas merge, initial
     * replaces — see {@link mergeRoomData}) and queue it for persistence
     * (debounced + coalesced).
     */
    put(roomId: string, data: MSC3575RoomData, receipt?: IMinimalEvent, accountData?: IMinimalEvent[]): void;
    /** Write all dirty records, then evict down to MAX_ROOMS by lowest bump. */
    flush(): Promise<void>;
    private evict;
    /** Drop the entire cache (used on schema change / reset). */
    clear(): Promise<void>;
    /** Flush any pending writes and stop accepting new ones. */
    stop(): Promise<void>;
}
//# sourceMappingURL=sliding-sync-cache.d.ts.map