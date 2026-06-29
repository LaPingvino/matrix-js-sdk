import { Room } from "./models/room.ts";
import { type IStoredClientOpts, type MatrixClient } from "./client.ts";
import { type ISyncStateData, SyncState, type SyncApiOptions, type SetPresence } from "./sync.ts";
import { type MatrixEvent } from "./models/event.ts";
import { SlidingSync } from "./sliding-sync.ts";
/**
 * A copy of SyncApi such that it can be used as a drop-in replacement for sync v2. For the actual
 * sliding sync API, see sliding-sync.ts or the class SlidingSync.
 */
export declare class SlidingSyncSdk {
    private readonly slidingSync;
    private readonly client;
    private readonly opts;
    private readonly syncOpts;
    private syncState;
    private syncStateData?;
    private lastPos;
    private failCount;
    /** Dedicated fast-poll connection for to_device + e2ee (see constructor). */
    private readonly encryptionSync?;
    /** Persistent per-room cache; replayed on boot so the UI paints before the network answers. */
    private readonly roomCache;
    private notifEvents;
    /** True while replaying cached rooms on boot, so onRoomData doesn't re-persist them. */
    private rehydrating;
    /**
     * Rooms that have received a genuine LIVE sliding-sync response this session (NOT a
     * cache rehydrate). Monotonic — a room never leaves once it's in. Consumers use this
     * to decide whether a room's data (e.g. its unread count) is trustworthy-current vs a
     * possibly-stale cached value: sliding sync loads rooms partially/incrementally, so a
     * rehydrated-but-not-yet-live room's count must be treated as provisional.
     */
    private readonly liveSyncedRooms;
    constructor(slidingSync: SlidingSync, client: MatrixClient, opts: IStoredClientOpts | undefined, syncOpts: SyncApiOptions);
    private onRoomData;
    private onLifecycle;
    /**
     * Lazily-created classic SyncApi used SOLELY for the isolated one-shot /sync that
     * fetches left rooms (see {@link syncLeftRooms}). It is never started (.sync() is
     * not called), so it runs no live loop and does not compete with the sliding-sync
     * transport — it only issues a single filtered request on demand.
     */
    private leftRoomsSyncApi?;
    /**
     * Sync rooms the user has left.
     *
     * Simplified sliding sync (MSC4186) has no "left rooms" section — its lists range
     * over JOINED rooms — so historical/archived rooms must be fetched explicitly. The
     * classic `/sync` endpoint is transport-independent and still serves a filtered
     * `include_leave` request, so we delegate to the classic SyncApi's well-tested
     * implementation (a single isolated `timeout=0` request that injects the left rooms
     * into the store) rather than duplicating the room-injection logic here. This is
     * what backs the "show archived/left rooms" affordance under sliding sync.
     *
     * @returns Resolved with the left rooms once they've been added to the store.
     */
    syncLeftRooms(): Promise<Room[]>;
    /**
     * Peek into a room. This will result in the room in question being synced so it
     * is accessible via getRooms(). Live updates for the room will be provided.
     * @param roomId - The room ID to peek into.
     * @returns A promise which resolves once the room has been added to the
     * store.
     */
    peek(roomId: string): Promise<Room>;
    /**
     * Stop polling for updates in the peeked room. NOPs if there is no room being
     * peeked.
     */
    stopPeeking(): void;
    /**
     * Specify the set_presence value to be used for subsequent calls to the Sync API.
     * @param presence - the presence to specify to set_presence of sync calls
     */
    setPresence(presence?: SetPresence): void;
    /**
     * Returns the current state of this sync object
     * @see MatrixClient#event:"sync"
     */
    getSyncState(): SyncState | null;
    /**
     * Returns the additional data object associated with
     * the current sync state, or null if there is no
     * such data.
     * Sync errors, if available, are put in the 'error' key of
     * this object.
     */
    getSyncStateData(): ISyncStateData | null;
    createRoom(roomId: string): Room;
    private registerStateListeners;
    private shouldAbortSync;
    private processRoomData;
    /**
     * Injects events into a room's model.
     * @param stateEventList - A list of state events. This is the state
     * at the *END* of the timeline list if it is supplied.
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     * @param numLive - the number of trailing events in timelineEventList which just
     * happened (and so should fire as live, not fromCache). Derived by the caller from
     * the sync stream — NOT taken from the server's num_live, which Continuwuity and
     * other non-Synapse servers leave unset. See processRoomData for the derivation.
     */
    injectRoomEvents(room: Room, stateEventList: MatrixEvent[], timelineEventList?: MatrixEvent[], numLive?: number): Promise<void>;
    private resolveInvites;
    retryImmediately(): boolean;
    /**
     * Whether the given room has received a genuine LIVE sliding-sync response this session
     * (as opposed to only being painted from the boot cache). Consumers use this to gate
     * trust in a room's current data — see {@link liveSyncedRooms}.
     */
    hasLiveSynced(roomId: string): boolean;
    /**
     * Paint from cache before the network answers: replay every persisted room's
     * data through the SAME ingestion path a live response takes ({@link onRoomData}
     * → {@link processRoomData}), so the rooms, timelines and state are reconstructed
     * identically to classic sync's `getSavedSync()` rehydrate — just sourced per-room
     * from {@link SlidingSyncCache} instead of one monolithic accumulator. Must run
     * BEFORE the live sync starts, so the live `initial=true` responses dedupe against
     * the rehydrated timeline rather than duplicating it. Best-effort: any failure
     * leaves us with today's cold start.
     */
    private rehydrateFromCache;
    /**
     * Main entry point. Blocks until stop() is called.
     */
    sync(): Promise<void>;
    /**
     * Stops the sync object from syncing.
     */
    stop(): void;
    /**
     * Sets the sync state and emits an event to say so
     * @param newState - The new state string
     * @param data - Object of additional data to emit in the event
     */
    private updateSyncState;
    /**
     * Takes a list of timelineEvents and adds and adds to notifEvents
     * as appropriate.
     * This must be called after the room the events belong to has been stored.
     *
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     */
    private addNotifications;
    /**
     * Purge any events in the notifEvents array. Used after a /sync has been complete.
     * This should not be called at a per-room scope (e.g in onRoomData) because otherwise the ordering
     * will be messed up e.g room A gets a bing, room B gets a newer bing, but both in the same /sync
     * response. If we purge at a per-room scope then we could process room B before room A leading to
     * room B appearing earlier in the notifications timeline, even though it has the higher origin_server_ts.
     */
    private purgeNotifications;
}
//# sourceMappingURL=sliding-sync-sdk.d.ts.map