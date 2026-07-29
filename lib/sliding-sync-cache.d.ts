import { type MSC3575RoomData } from "./sliding-sync.ts";
import { type IMinimalEvent } from "./sync-accumulator.ts";
import { type Logger } from "./logger.ts";
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
export declare function isSpaceData(data: MSC3575RoomData): boolean;
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
export declare function shellRecord(rec: CachedRoomRecord): CachedRoomRecord;
/**
 * Decide what to shell and what to delete. Pure; exported for tests.
 * See {@link SlidingSyncCache.prune} for the reasoning behind the two caps.
 */
export declare function planPrune(records: CachedRoomRecord[]): {
    shell: string[];
    drop: string[];
};
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
    /** Records to delete from IDB on the next flush (see {@link prune}). */
    private deletes;
    /** Set when a record was genuinely deleted — this session or a previous one. */
    private dropped;
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
    get droppedRecords(): boolean;
    /** Acknowledge {@link droppedRecords}: the caller has dropped the pos. */
    clearDropped(): Promise<void>;
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
    private prune;
    /** Prune to the caps, then write everything pending in one transaction. */
    flush(): Promise<void>;
    /** Drop the entire cache (used on schema change / reset). */
    clear(): Promise<void>;
    /** Flush any pending writes and stop accepting new ones. */
    stop(): Promise<void>;
}
//# sourceMappingURL=sliding-sync-cache.d.ts.map