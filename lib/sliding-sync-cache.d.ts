import { type MSC3575RoomData } from "./sliding-sync.ts";
import { type IMinimalEvent } from "./sync-accumulator.ts";
import { type Logger } from "./logger.ts";
export declare class SlidingSyncCache {
    private readonly logger;
    private dbPromise;
    private pending;
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
    }[]>;
    /** Queue a room's data for persistence (debounced + coalesced). */
    put(roomId: string, data: MSC3575RoomData, receipt?: IMinimalEvent): void;
    /** Write all pending records, then evict down to MAX_ROOMS by lowest bump. */
    flush(): Promise<void>;
    private evict;
    /** Drop the entire cache (used on schema change / reset). */
    clear(): Promise<void>;
    /** Flush any pending writes and stop accepting new ones. */
    stop(): Promise<void>;
}
//# sourceMappingURL=sliding-sync-cache.d.ts.map