/*
Copyright 2022 The Matrix.org Foundation C.I.C.

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

import type { SyncCryptoCallbacks } from "./common-crypto/CryptoBackend.ts";
import { NotificationCountType, Room, RoomEvent } from "./models/room.ts";
import { logger } from "./logger.ts";
import { promiseMapSeries } from "./utils.ts";
import { EventTimeline } from "./models/event-timeline.ts";
import { ClientEvent, type IStoredClientOpts, type MatrixClient } from "./client.ts";
import {
    type ISyncStateData,
    SyncState,
    SyncApi,
    _createAndReEmitRoom,
    type SyncApiOptions,
    defaultClientOpts,
    defaultSyncApiOpts,
    type SetPresence,
    processToDeviceMessages,
} from "./sync.ts";
import { type MatrixEvent } from "./models/event.ts";
import {
    type IMinimalEvent,
    type IRoomEvent,
    type IStateEvent,
    type IStrippedState,
    type ISyncResponse,
    type ReceivedToDeviceMessage,
} from "./sync-accumulator.ts";
import { MatrixError } from "./http-api/index.ts";
import {
    type Extension,
    ExtensionState,
    type MSC3575RoomData,
    type MSC3575SlidingSyncResponse,
    SlidingSync,
    SlidingSyncEvent,
    SlidingSyncState,
    DEFAULT_SLIDING_SYNC_REQUIRED_STATE,
} from "./sliding-sync.ts";
import { SlidingSyncCache } from "./sliding-sync-cache.ts";
import { EventType, UNSTABLE_ELEMENT_FUNCTIONAL_USERS } from "./@types/event.ts";
import { type IPushRules } from "./@types/PushRules.ts";
import { RoomStateEvent } from "./models/room-state.ts";
import { RoomMemberEvent } from "./models/room-member.ts";
import { KnownMembership, type Membership } from "./@types/membership.ts";

// Number of consecutive failed syncs that will lead to a syncState of ERROR as opposed
// to RECONNECTING. This is needed to inform the client of server issues when the
// keepAlive is successful but the server /sync fails.
const FAILED_SYNC_ERROR_THRESHOLD = 3;

/** Poll timeout for the dedicated encryption sync. The server long-polls on a
 * per-user watcher and WAKES on new data (to-device, device-list changes, OTK
 * counts are all watched), so delivery latency is wake-driven, not poll-driven
 * — a long timeout just cuts idle request volume. The server caps the hang at
 * 30s. (The old 3s poll dated from the disproven "the server never wakes"
 * model.) During a to-device handshake the boost mechanism in
 * {@link SlidingSync#start} still fast-polls as a belt-and-braces. */
const ENCRYPTION_SYNC_TIMEOUT_MS = 30_000;

/** Name of the lean room subscription used to bulk-materialise DM rooms (just
 * enough state to show them in the list, NOT the heavy opened-room set). */
const DM_MATERIALIZE_SUB = "lean-materialize";

type ExtensionE2EERequest = {
    enabled: boolean;
};

type ExtensionE2EEResponse = Pick<
    ISyncResponse,
    | "device_lists"
    | "device_one_time_keys_count"
    | "device_unused_fallback_key_types"
    | "org.matrix.msc2732.device_unused_fallback_key_types"
>;

class ExtensionE2EE implements Extension<ExtensionE2EERequest, ExtensionE2EEResponse> {
    public constructor(private readonly crypto: SyncCryptoCallbacks) {}

    public name(): string {
        return "e2ee";
    }

    public when(): ExtensionState {
        return ExtensionState.PreProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionE2EERequest> {
        if (isInitial) {
            // In SSS, the `?pos=` contains the stream position for device list updates.
            // If we do not have a `?pos=` (e.g because we forgot it, or because the server
            // invalidated our connection) then we MUST invlaidate all device lists because
            // the server will not tell us the delta. This will then cause UTDs as we will fail
            // to encrypt for new devices. This is an expensive call, so we should
            // really really remember `?pos=` wherever possible.
            logger.log("ExtensionE2EE: invalidating all device lists due to missing 'pos'");
            await this.crypto.markAllTrackedUsersAsDirty();
        }
        return {
            enabled: true, // this is sticky so only send it on the initial request
        };
    }

    public async onResponse(data: ExtensionE2EEResponse): Promise<void> {
        // Handle device list updates
        if (data.device_lists) {
            await this.crypto.processDeviceLists(data.device_lists);
        }

        // Handle one_time_keys_count and unused_fallback_key_types
        await this.crypto.processKeyCounts(
            data.device_one_time_keys_count,
            data["device_unused_fallback_key_types"] || data["org.matrix.msc2732.device_unused_fallback_key_types"],
        );

        // AWAIT: drain the outgoing-request pump before this cycle returns, so the next
        // (fast) encryption-sync poll's to-device receive does not overlap the pump on the
        // single non-reentrant OlmMachine — which corrupts in-flight SAS (spurious
        // m.mismatched_sas). See RustCrypto.onSyncCompleted.
        await this.crypto.onSyncCompleted({});
    }
}

type ExtensionToDeviceRequest = {
    since?: string;
    limit?: number;
    enabled?: boolean;
};

type ExtensionToDeviceResponse = {
    events: Required<ISyncResponse>["to_device"]["events"];
    next_batch: string | null;
};

class ExtensionToDevice implements Extension<ExtensionToDeviceRequest, ExtensionToDeviceResponse> {
    private nextBatch: string | null = null;

    public constructor(
        private readonly client: MatrixClient,
        private readonly cryptoCallbacks?: SyncCryptoCallbacks,
    ) {}

    public name(): string {
        return "to_device";
    }

    public when(): ExtensionState {
        return ExtensionState.PreProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionToDeviceRequest> {
        return {
            since: this.nextBatch !== null ? this.nextBatch : undefined,
            limit: 100,
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionToDeviceResponse): Promise<void> {
        const events = data["events"] || [];
        let receivedToDeviceMessages: ReceivedToDeviceMessage[];
        if (this.cryptoCallbacks) {
            receivedToDeviceMessages = await this.cryptoCallbacks.preprocessToDeviceMessages(events);
        } else {
            receivedToDeviceMessages = events.map((rawEvent) =>
                // Crypto is not enabled, so we just return the events.
                ({
                    message: rawEvent,
                    encryptionInfo: null,
                }),
            );
        }
        processToDeviceMessages(receivedToDeviceMessages, this.client);

        this.nextBatch = data.next_batch;
    }
}

type ExtensionAccountDataRequest = {
    enabled: boolean;
};

type ExtensionAccountDataResponse = {
    global: IMinimalEvent[];
    rooms: Record<string, IMinimalEvent[]>;
};

class ExtensionAccountData implements Extension<ExtensionAccountDataRequest, ExtensionAccountDataResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "account_data";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionAccountDataRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionAccountDataResponse): Promise<void> {
        if (data.global && data.global.length > 0) {
            this.processGlobalAccountData(data.global);
        }

        for (const roomId in data.rooms) {
            const accountDataEvents = mapEvents(this.client, roomId, data.rooms[roomId]);
            const room = this.client.getRoom(roomId);
            if (!room) {
                // Expected under sliding sync: extensions can carry data for
                // rooms outside the current window. Not an error.
                logger.debug("got account data for room but room doesn't exist on client:", roomId);
                continue;
            }
            room.addAccountData(accountDataEvents);
            accountDataEvents.forEach((e) => {
                this.client.emit(ClientEvent.Event, e);
            });
        }
    }

    private processGlobalAccountData(globalAccountData: IMinimalEvent[]): void {
        const events = mapEvents(this.client, undefined, globalAccountData);
        const prevEventsMap = events.reduce<Record<string, MatrixEvent | undefined>>((m, c) => {
            m[c.getType()] = this.client.store.getAccountData(c.getType());
            return m;
        }, {});
        this.client.store.storeAccountDataEvents(events);
        events.forEach((accountDataEvent) => {
            // Honour push rules that come down the sync stream but also
            // honour push rules that were previously cached. Base rules
            // will be updated when we receive push rules via getPushRules
            // (see sync) before syncing over the network.
            if (accountDataEvent.getType() === EventType.PushRules) {
                const rules = accountDataEvent.getContent<IPushRules>();
                // Only re-apply push rules when they actually changed. The
                // server can resend global account data on every sliding-sync
                // response, and re-running setPushRules each time is wasteful
                // (rewriteDefaultRules rebuilds the rule set) and floods logs
                // with "Missing/Adding default global ... push rule".
                const prevRules = prevEventsMap[EventType.PushRules]?.getContent<IPushRules>();
                if (!prevRules || JSON.stringify(prevRules) !== JSON.stringify(rules)) {
                    this.client.setPushRules(rules);
                }
            }
            const prevEvent = prevEventsMap[accountDataEvent.getType()];
            this.client.emit(ClientEvent.AccountData, accountDataEvent, prevEvent);
            return accountDataEvent;
        });
    }
}

type ExtensionTypingRequest = {
    enabled: boolean;
};

type ExtensionTypingResponse = {
    rooms: Record<string, IMinimalEvent>;
};

class ExtensionTyping implements Extension<ExtensionTypingRequest, ExtensionTypingResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "typing";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionTypingRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionTypingResponse): Promise<void> {
        if (!data?.rooms) {
            return;
        }

        for (const roomId in data.rooms) {
            processEphemeralEvents(this.client, roomId, [data.rooms[roomId]]);
        }
    }
}

type ExtensionReceiptsRequest = {
    enabled: boolean;
};

type ExtensionReceiptsResponse = {
    rooms: Record<string, IMinimalEvent>;
};

class ExtensionReceipts implements Extension<ExtensionReceiptsRequest, ExtensionReceiptsResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "receipts";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionReceiptsRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionReceiptsResponse): Promise<void> {
        if (!data?.rooms) {
            return;
        }

        for (const roomId in data.rooms) {
            processEphemeralEvents(this.client, roomId, [data.rooms[roomId]]);
        }
    }
}

/**
 * A copy of SyncApi such that it can be used as a drop-in replacement for sync v2. For the actual
 * sliding sync API, see sliding-sync.ts or the class SlidingSync.
 */
export class SlidingSyncSdk {
    private readonly opts: IStoredClientOpts;
    private readonly syncOpts: SyncApiOptions;
    private syncState: SyncState | null = null;
    private syncStateData?: ISyncStateData;
    private lastPos: string | null = null;
    private failCount = 0;
    /** Dedicated fast-poll connection for to_device + e2ee (see constructor). */
    private readonly encryptionSync?: SlidingSync;
    /** Persistent per-room cache; replayed on boot so the UI paints before the network answers. */
    private readonly roomCache: SlidingSyncCache;
    private notifEvents: MatrixEvent[] = []; // accumulator of sync events in the current sync response
    /** True while replaying cached rooms on boot, so onRoomData doesn't re-persist them. */
    private rehydrating = false;
    /**
     * Rooms that have received a genuine LIVE sliding-sync response this session (NOT a
     * cache rehydrate). Monotonic — a room never leaves once it's in. Consumers use this
     * to decide whether a room's data (e.g. its unread count) is trustworthy-current vs a
     * possibly-stale cached value: sliding sync loads rooms partially/incrementally, so a
     * rehydrated-but-not-yet-live room's count must be treated as provisional.
     */
    private readonly liveSyncedRooms = new Set<string>();
    /** Last wall-clock we requested a connection re-init for a delta targeting an unknown room. */
    private lastUnknownRoomReinit = 0;

    public constructor(
        private readonly slidingSync: SlidingSync,
        private readonly client: MatrixClient,
        opts: IStoredClientOpts | undefined,
        syncOpts: SyncApiOptions,
    ) {
        this.opts = defaultClientOpts(opts);
        this.syncOpts = defaultSyncApiOpts(syncOpts);
        this.roomCache = new SlidingSyncCache(this.client.getUserId() ?? undefined, this.syncOpts.logger);

        if (client.getNotifTimelineSet()) {
            client.reEmitter.reEmit(client.getNotifTimelineSet()!, [RoomEvent.Timeline, RoomEvent.TimelineReset]);
        }

        this.slidingSync.on(SlidingSyncEvent.Lifecycle, this.onLifecycle.bind(this));
        this.slidingSync.on(SlidingSyncEvent.RoomData, this.onRoomData.bind(this));

        // A room we are no longer in is a room the server has stopped sending
        // (MSC4186 lists carry joined/invited/knocked only), so a cached record for
        // it can never be refreshed OR invalidated by the network again — it would
        // just replay on every boot, resurrecting a declined invite or a left room.
        // Forget it the moment membership says we are out.
        this.client.on(RoomEvent.MyMembership, (room, membership) => {
            if (membership === KnownMembership.Leave || membership === KnownMembership.Ban) {
                this.roomCache.forget(room.roomId);
                this.liveSyncedRooms.delete(room.roomId);
            }
        });
        // The room/list sync carries the non-latency-critical extensions.
        const mainExtensions: Extension<any, any>[] = [
            new ExtensionAccountData(this.client),
            new ExtensionTyping(this.client),
            new ExtensionReceipts(this.client),
        ];
        mainExtensions.forEach((ext) => {
            this.slidingSync.registerExtension(ext);
        });

        // Dedicated ENCRYPTION sync: a SECOND connection (its own conn_id) with no
        // room lists, carrying only the to_device + e2ee extensions, polled fast.
        // This keeps latency-sensitive crypto — verification handshakes, room-key
        // shares, device-list updates — off the slow room long-poll, mirroring the
        // Rust SDK / Element X two-connection design. Without it, to-device sits
        // behind the room poll and "immediate" things (verification, UTD recovery)
        // lag by a whole poll cycle each step.
        this.encryptionSync = new SlidingSync(
            this.client.baseUrl,
            new Map(),
            { timeline_limit: 0, required_state: [] },
            this.client,
            ENCRYPTION_SYNC_TIMEOUT_MS,
            "encryption",
        );
        const cryptoExtensions: Extension<any, any>[] = [
            // to_device is delivered even without crypto (it just won't decrypt).
            new ExtensionToDevice(this.client, this.syncOpts.cryptoCallbacks),
        ];
        if (this.syncOpts.cryptoCallbacks) {
            cryptoExtensions.push(new ExtensionE2EE(this.syncOpts.cryptoCallbacks));
        }
        cryptoExtensions.forEach((ext) => {
            this.encryptionSync!.registerExtension(ext);
        });
    }

    private async onRoomData(roomId: string, roomData: MSC3575RoomData): Promise<void> {
        try {
            let room = this.client.store.getRoom(roomId);
            if (!room) {
                if (!roomData.initial) {
                    // A non-initial response is a DELTA against state the server
                    // believes we hold (conn_id + resumed pos) — but we don't have
                    // this room (boot cache evicted/wiped it while the pos
                    // survived). Ingesting the delta would create a state-degraded
                    // room (no m.room.create, possibly no m.room.encryption — a
                    // plaintext-to-E2EE hazard), and dropping it silently would
                    // leave the room invisible FOREVER (the server never re-sends
                    // what it thinks we have). The only sound recovery is to start
                    // the connection over: since=0 makes the server forget this
                    // conn_id and re-send everything initial:true. Rate-limited so
                    // a pathological loop can't hammer the server; one re-init
                    // recreates every room, so it converges after a single pass.
                    const now = Date.now();
                    if (now - this.lastUnknownRoomReinit > 5 * 60 * 1000) {
                        this.lastUnknownRoomReinit = now;
                        this.syncOpts.logger.warn(
                            `Received a delta for unknown room ${roomId}; local state is behind the ` +
                                `connection — reinitialising the sliding-sync connection`,
                        );
                        this.slidingSync.reinitialize();
                    } else {
                        this.syncOpts.logger.debug(
                            "delta for unknown room (reinit already requested recently), skipping",
                            roomId,
                        );
                    }
                    return;
                }
                room = _createAndReEmitRoom(this.client, roomId, this.opts);
            }
            await this.processRoomData(this.client, room!, roomData);
            // Remember this room's data so the next boot can paint it before the
            // network answers. Skipped while replaying (the data came FROM cache).
            // The read-receipt snapshot below is now a SECONDARY nicety (it gives the
            // open room a correct marker on first paint); the primary stale-unread fix
            // is consumer-side confidence gating off `liveSyncedRooms` (see
            // hasLiveSynced / MatrixClient.isRoomLiveSynced).
            if (!this.rehydrating) {
                // This room now has verifiably-current data this session.
                this.liveSyncedRooms.add(roomId);
                let receipt: IMinimalEvent | undefined;
                const uid = this.client.getUserId();
                // Source the marker from getEventReadUpTo — the SAME accessor the
                // unread UI uses. It returns the LATEST read position across public,
                // PRIVATE and synthetic receipts. getReadReceiptForUserId alone only
                // sees the public m.read type, so a user whose latest receipt is a
                // private one would get a STALE marker persisted — silently bringing
                // the flicker back for exactly those users. We replay it as a plain
                // unthreaded public receipt: type doesn't matter for unread, only that
                // it lands on the same (latest) event so the reloaded count matches.
                const readUpToId = uid ? room!.getEventReadUpTo(uid, false) : null;
                if (uid && readUpToId) {
                    const ts = room!.findEventById(readUpToId)?.getTs() ?? 0;
                    receipt = {
                        type: "m.receipt",
                        content: { [readUpToId]: { "m.read": { [uid]: { ts } } } },
                    } as IMinimalEvent;
                }
                // Persist the room's per-room account_data (m.tag favourites,
                // m.marked_unread, …) too: it rides a separate extension and isn't in
                // MSC3575RoomData, so without this the rehydrated room loses room.tags
                // and favourites revert on reload until the live account_data arrives.
                const accountData: IMinimalEvent[] = Array.from(room!.accountData.values()).map(
                    (e) => ({ type: e.getType(), content: e.getContent() }) as IMinimalEvent,
                );
                this.roomCache.put(roomId, roomData, receipt, accountData.length ? accountData : undefined);
            }
        } catch (e) {
            // Resilience: one malformed room must not break sliding sync (it
            // arrives per-room here, so an unhandled rejection would otherwise
            // surface as a spurious error and skip nothing useful).
            this.syncOpts.logger.error(`Failed to process sliding-sync data for room ${roomId}; skipping`, e);
        }
    }

    private onLifecycle(state: SlidingSyncState, resp: MSC3575SlidingSyncResponse | null, err?: Error): void {
        if (err) {
            this.syncOpts.logger.debug("onLifecycle", state, err);
        }
        switch (state) {
            case SlidingSyncState.Complete:
                this.purgeNotifications();
                if (!resp) {
                    break;
                }
                // Element won't stop showing the initial loading spinner unless we fire SyncState.Prepared
                if (!this.lastPos) {
                    this.updateSyncState(SyncState.Prepared, {
                        oldSyncToken: undefined,
                        nextSyncToken: resp.pos,
                        catchingUp: false,
                        fromCache: false,
                    });
                }
                // Conversely, Element won't show the room list unless there is at least 1x SyncState.Syncing
                // so hence for the very first sync we will fire prepared then immediately syncing.
                this.updateSyncState(SyncState.Syncing, {
                    oldSyncToken: this.lastPos!,
                    nextSyncToken: resp.pos,
                    catchingUp: false,
                    fromCache: false,
                });
                this.lastPos = resp.pos;
                break;
            case SlidingSyncState.RequestFinished:
                if (err) {
                    this.failCount += 1;
                    this.updateSyncState(
                        this.failCount > FAILED_SYNC_ERROR_THRESHOLD ? SyncState.Error : SyncState.Reconnecting,
                        {
                            error: new MatrixError(err),
                        },
                    );
                    if (this.shouldAbortSync(new MatrixError(err))) {
                        return; // shouldAbortSync actually stops syncing too so we don't need to do anything.
                    }
                } else {
                    this.failCount = 0;
                    this.syncOpts.logger.debug(
                        `SlidingSyncState.RequestFinished with ${Object.keys(resp?.rooms || []).length} rooms`,
                    );
                }
                break;
        }
    }

    /**
     * Lazily-created classic SyncApi used SOLELY for the isolated one-shot /sync that
     * fetches left rooms (see {@link syncLeftRooms}). It is never started (.sync() is
     * not called), so it runs no live loop and does not compete with the sliding-sync
     * transport — it only issues a single filtered request on demand.
     */
    private leftRoomsSyncApi?: SyncApi;

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
    public async syncLeftRooms(): Promise<Room[]> {
        if (!this.leftRoomsSyncApi) {
            this.leftRoomsSyncApi = new SyncApi(this.client, this.opts, this.syncOpts);
        }
        return this.leftRoomsSyncApi.syncLeftRooms();
    }

    /**
     * Peek into a room. This will result in the room in question being synced so it
     * is accessible via getRooms(). Live updates for the room will be provided.
     * @param roomId - The room ID to peek into.
     * @returns A promise which resolves once the room has been added to the
     * store.
     */
    public async peek(roomId: string): Promise<Room> {
        return null!; // TODO
    }

    /**
     * Stop polling for updates in the peeked room. NOPs if there is no room being
     * peeked.
     */
    public stopPeeking(): void {
        // TODO
    }

    /**
     * Specify the set_presence value to be used for subsequent calls to the Sync API.
     * @param presence - the presence to specify to set_presence of sync calls
     */
    public setPresence(presence?: SetPresence): void {
        // TODO not possible in sliding sync yet
    }

    /**
     * Returns the current state of this sync object
     * @see MatrixClient#event:"sync"
     */
    public getSyncState(): SyncState | null {
        return this.syncState;
    }

    /**
     * Returns the additional data object associated with
     * the current sync state, or null if there is no
     * such data.
     * Sync errors, if available, are put in the 'error' key of
     * this object.
     */
    public getSyncStateData(): ISyncStateData | null {
        return this.syncStateData ?? null;
    }

    // Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts

    public createRoom(roomId: string): Room {
        // XXX cargoculted from sync.ts
        const { timelineSupport } = this.client;
        const room = new Room(roomId, this.client, this.client.getUserId()!, {
            lazyLoadMembers: this.opts.lazyLoadMembers,
            pendingEventOrdering: this.opts.pendingEventOrdering,
            timelineSupport,
        });
        this.client.reEmitter.reEmit(room, [
            RoomEvent.Name,
            RoomEvent.Redaction,
            RoomEvent.RedactionCancelled,
            RoomEvent.Receipt,
            RoomEvent.Tags,
            RoomEvent.LocalEchoUpdated,
            RoomEvent.AccountData,
            RoomEvent.MyMembership,
            RoomEvent.Timeline,
            RoomEvent.TimelineReset,
            RoomEvent.UnreadNotifications,
        ]);
        this.registerStateListeners(room);
        return room;
    }

    private registerStateListeners(room: Room): void {
        // XXX cargoculted from sync.ts
        // we need to also re-emit room state and room member events, so hook it up
        // to the client now. We need to add a listener for RoomState.members in
        // order to hook them correctly.
        this.client.reEmitter.reEmit(room.currentState, [
            RoomStateEvent.Events,
            RoomStateEvent.Members,
            RoomStateEvent.NewMember,
            RoomStateEvent.Update,
        ]);
        room.currentState.on(RoomStateEvent.NewMember, (event, state, member) => {
            member.user = this.client.getUser(member.userId) ?? undefined;
            this.client.reEmitter.reEmit(member, [
                RoomMemberEvent.Name,
                RoomMemberEvent.Typing,
                RoomMemberEvent.PowerLevel,
                RoomMemberEvent.Membership,
            ]);
        });
    }

    /*
    private deregisterStateListeners(room: Room): void { // XXX cargoculted from sync.ts
        // could do with a better way of achieving this.
        room.currentState.removeAllListeners(RoomStateEvent.Events);
        room.currentState.removeAllListeners(RoomStateEvent.Members);
        room.currentState.removeAllListeners(RoomStateEvent.NewMember);
    } */

    private shouldAbortSync(error: MatrixError): boolean {
        if (error.errcode === "M_UNKNOWN_TOKEN") {
            // The logout already happened, we just need to stop.
            this.syncOpts.logger.warn("Token no longer valid - assuming logout");
            this.stop();
            this.updateSyncState(SyncState.Error, { error });
            return true;
        }
        return false;
    }

    private async processRoomData(client: MatrixClient, room: Room, roomData: MSC3575RoomData): Promise<void> {
        // Only store the room the first time we see it. The server re-sends rooms
        // with initial=true whenever they (re-)enter a sliding window (e.g. as
        // the range grows), and store.storeRoom() registers a fresh
        // RoomState.members listener each call — so re-storing leaks listeners
        // (MaxListenersExceededWarning) on large accounts. State still updates
        // via injectRoomEvents regardless.
        const newToStore = !client.store.getRoom(room.roomId);
        roomData = ensureNameEvent(client, room.roomId, roomData);
        const stateEvents = mapEvents(this.client, room.roomId, roomData.required_state);
        // Prevent events from being decrypted ahead of time
        // this helps large account to speed up faster
        // room::decryptCriticalEvent is in charge of decrypting all the events
        // required for a client to function properly
        let timelineEvents = mapEvents(this.client, room.roomId, roomData.timeline, false);
        const ephemeralEvents: MatrixEvent[] = []; // TODO this.mapSyncEventsFormat(joinObj.ephemeral);

        // TODO: handle threaded / beacon events

        // Bucket the received window against what we already hold. Computed
        // BEFORE any timeline mutation below.
        const liveTimelineEvents = room.getLiveTimeline().getEvents();
        const hadEvents = liveTimelineEvents.length > 0;
        let didReset = false;
        if (roomData.limited || roomData.initial) {
            // we should not know about any of these timeline entries if this is a genuinely new room.
            // If we do, then we've effectively done scrollback (e.g requesting timeline_limit: 1 for
            // this room, then timeline_limit: 50).
            const knownEvents = new Set<string>();
            liveTimelineEvents.forEach((e) => {
                knownEvents.add(e.getId()!);
            });
            const anyKnown = timelineEvents.some((e) => knownEvents.has(e.getId()!));
            if (roomData.limited && hadEvents && !anyKnown) {
                // A LIMITED window sharing NOTHING with what we hold: more events
                // arrived than the window could carry, and the whole window is
                // past our tail — a REAL GAP between our newest event and the
                // window's oldest (bridge bursts, catch-up after offline, re-init
                // after session expiry). Classic sync's answer, taken verbatim:
                // reset the live timeline. The window's events then append as
                // LIVE events on the fresh timeline (visible, notifying), the old
                // timeline stays reachable, and the gap becomes back-PAGINATABLE
                // via prev_batch instead of silently unreachable (the pre-conn_id
                // "lesser evil" was to append gap-events as live and never
                // deliver the gap at all). Both clients handle the reset:
                // Wally's RoomTimeline self-heals on RoomEvent.TimelineReset and
                // WukkieMail re-snaps from getLiveTimeline() every sync.
                room.resetLiveTimeline(roomData.prev_batch ?? null, null);
                // A gap means incremental notif tracking is broken; same as sync.ts.
                this.client.resetNotifTimelineSet();
                didReset = true;
            } else {
                // Unknown events BEFORE the OLDEST known event are scrollback e.g:
                //       D E   <-- what we know
                // A B C D E F <-- what we just received
                // means:
                // A B C       <-- scrollback
                //       D E   <-- dupes
                //           F <-- new event
                //
                // The anchor MUST be the oldest known event, not the newest. Under
                // chronological pendingEventOrdering our own just-sent message is
                // already in the live timeline with its real id — it is the NEWEST
                // known event. Anchoring on the newest (as the original upstream
                // bucketing did) classified every concurrent foreign event ordered
                // before our echo as "scrollback" and PREPENDED it to the top of the
                // timeline — invisible until a full reload rebuilt the room. Bridge
                // bursts right after a send/reaction hit this constantly. Unknown
                // events between/after known events are treated as live: worst case
                // they append slightly out of order (the display layers sort), which
                // beats hiding them at the start of the timeline.
                // NO overlap at all with what we hold? Then this is a brand-new /
                // empty room (the gap case above already peeled off limited windows
                // over a non-empty timeline): everything is live.
                const oldEvents: MatrixEvent[] = [];
                const newEvents: MatrixEvent[] = [];
                let seenKnownEvent = false;
                for (const recvEvent of timelineEvents) {
                    // oldest -> newest
                    if (knownEvents.has(recvEvent.getId()!)) {
                        seenKnownEvent = true;
                        continue; // don't include this event, it's a dupe
                    }
                    if (seenKnownEvent || !anyKnown) {
                        // newer than the oldest event we already hold (or no anchor
                        // at all): live, not scrollback
                        newEvents.push(recvEvent);
                    } else {
                        // older than everything we hold: scrollback.
                        // unshift => reverse-chronological, the order
                        // addEventsToTimeline(toStartOfTimeline) expects.
                        oldEvents.unshift(recvEvent);
                    }
                }
                timelineEvents = newEvents;
                if (oldEvents.length > 0) {
                    // old events are scrollback, insert them now
                    room.addEventsToTimeline(oldEvents, true, false, room.getLiveTimeline(), roomData.prev_batch);
                }
            }
        }

        const encrypted = room.hasEncryptionStateEvent();
        // we do this first so it's correct when any of the events fire
        if (roomData.notification_count != null) {
            room.setUnreadNotificationCount(NotificationCountType.Total, roomData.notification_count);
        }

        if (roomData.highlight_count != null) {
            // We track unread notifications ourselves in encrypted rooms, so don't
            // bother setting it here. We trust our calculations better than the
            // server's for this case, and therefore will assume that our non-zero
            // count is accurate.
            if (!encrypted || (encrypted && room.getUnreadNotificationCount(NotificationCountType.Highlight) <= 0)) {
                room.setUnreadNotificationCount(NotificationCountType.Highlight, roomData.highlight_count);
            }
        }
        if (roomData.bump_stamp) {
            room.setBumpStamp(roomData.bump_stamp);
        }

        if (Number.isInteger(roomData.invited_count)) {
            room.currentState.setInvitedMemberCount(roomData.invited_count!);
        }
        if (Number.isInteger(roomData.joined_count)) {
            room.currentState.setJoinedMemberCount(roomData.joined_count!);
            // UTD guard: if this is an encrypted room whose full member roster we
            // already loaded, but the server now reports MORE joined members than
            // we actually hold, a user joined whose m.room.member event the lean
            // required_state never delivered. Our cached roster is stale, so the
            // next encrypt would share the megolm key to an INCOMPLETE recipient
            // set and the missing member(s) UTD on every device. Re-arm member
            // loading (Rust's mark_members_missing analogue) so the next key
            // share re-fetches /members and includes them.
            if (encrypted && room.membersLoaded() && roomData.joined_count! > room.getJoinedMembers().length) {
                room.invalidateLoadedMembers();
            }
        }

        if (roomData.invite_state) {
            const inviteStateEvents = mapEvents(this.client, room.roomId, roomData.invite_state);
            await this.injectRoomEvents(room, inviteStateEvents);
            // THE SERVER SENDING invite_state IS ITSELF THE STATEMENT "you are invited
            // here" — derive membership from that, don't re-derive it from the stripped
            // state's contents. Room.updateMyMembership is otherwise driven purely by an
            // m.room.member event with OUR user id in currentState, and stripped state is
            // exactly where servers disagree: Synapse reliably includes the invitee's own
            // member event, other implementations may send only the inviter's, omit the
            // state_key, or send a near-empty invite_state. Every one of those produced an
            // INVISIBLE invite — a room in the store that no consumer could see, because
            // membership stayed at whatever it was (usually undefined).
            //
            // A self member event is still authoritative when it says something MORE
            // specific than "invite" (a leave/ban that raced ahead of us), so it wins when
            // present and valid.
            const inviteSelfId = this.client.getUserId();
            const inviteSelfMember = inviteSelfId
                ? room.currentState.getStateEvents(EventType.RoomMember, inviteSelfId)
                : null;
            const strippedMembership = (inviteSelfMember?.getContent() as { membership?: string } | undefined)
                ?.membership;
            room.updateMyMembership((strippedMembership as Membership) ?? KnownMembership.Invite);
            if (roomData.initial && newToStore) {
                room.recalculate();
                this.client.store.storeRoom(room);
                this.client.emit(ClientEvent.Room, room);
            }
            inviteStateEvents.forEach((e) => {
                this.client.emit(ClientEvent.Event, e);
            });
            return;
        }

        if (roomData.limited && !didReset && !hadEvents) {
            // First paint of a fresh timeline: set the back-pagination token
            // *before* adding any events so clients can start back-paginating.
            // Only then — a reset already set the fresh timeline's token, and
            // when we HOLD older events the timeline's existing token is the
            // right deeper-history continuation; overwriting it with this
            // window's prev_batch (which points just before the window, i.e.
            // AHEAD of our timeline start) would make back-pagination re-fetch
            // events we already hold and skip the genuinely older history.
            room.getLiveTimeline().setPaginationToken(roomData.prev_batch ?? null, EventTimeline.BACKWARDS);
        }

        /* TODO
        else if (roomData.limited) {

            let limited = true;

            // we've got a limited sync, so we *probably* have a gap in the
            // timeline, so should reset. But we might have been peeking or
            // paginating and already have some of the events, in which
            // case we just want to append any subsequent events to the end
            // of the existing timeline.
            //
            // This is particularly important in the case that we already have
            // *all* of the events in the timeline - in that case, if we reset
            // the timeline, we'll end up with an entirely empty timeline,
            // which we'll try to paginate but not get any new events (which
            // will stop us linking the empty timeline into the chain).
            //
            for (let i = timelineEvents.length - 1; i >= 0; i--) {
                const eventId = timelineEvents[i].getId();
                if (room.getTimelineForEvent(eventId)) {
                    this.syncOpts.logger.debug("Already have event " + eventId + " in limited " +
                        "sync - not resetting");
                    limited = false;

                    // we might still be missing some of the events before i;
                    // we don't want to be adding them to the end of the
                    // timeline because that would put them out of order.
                    timelineEvents.splice(0, i);

                    // XXX: there's a problem here if the skipped part of the
                    // timeline modifies the state set in stateEvents, because
                    // we'll end up using the state from stateEvents rather
                    // than the later state from timelineEvents. We probably
                    // need to wind stateEvents forward over the events we're
                    // skipping.
                    break;
                }
            }

            if (limited) {
                room.resetLiveTimeline(
                    roomData.prev_batch,
                    null, // TODO this.syncOpts.canResetEntireTimeline(room.roomId) ? null : syncEventData.oldSyncToken,
                );

                // We have to assume any gap in any timeline is
                // reason to stop incrementally tracking notifications and
                // reset the timeline.
                this.client.resetNotifTimelineSet();
                this.registerStateListeners(room);
            }
        } */

        // Liveness is not the server's to declare. Continuwuity sends num_live: null, which
        // Element's code read as "0 live" → every event became fromCache:true → RoomEvent.Timeline
        // fired with liveEvent:false → consumers skip unread/notify/sound/reorder, so bridged
        // messages arrived silently. We derive it instead: the dedup split above already peeled
        // off scrollback, so what's left just happened — UNLESS this is the room's first paint
        // this session (cold catch-up or cache replay), which is history, not new activity.
        // Keyed off liveSyncedRooms (false only on the first live response), NOT roomData.initial,
        // which Continuwuity also sets when a KNOWN room merely re-enters the window.
        const firstPaint = !this.liveSyncedRooms.has(room.roomId);
        await this.injectRoomEvents(room, stateEvents, timelineEvents, firstPaint);

        // we deliberately don't add ephemeral events to the timeline
        room.addEphemeralEvents(ephemeralEvents);

        // local fields must be set before any async calls because call site assumes
        // synchronous execution prior to emitting SlidingSyncState.Complete.
        // Derive our membership from the m.room.member state we just injected
        // rather than assuming Join. Sliding-sync lists are over joined rooms, so
        // Join is the right DEFAULT, but a subscribed room (e.g. a space child we
        // are only previewing) or a room whose required_state carries a leave/ban
        // for us must reflect that — otherwise consumers see a not-joined room as
        // joined. Falls back to Join when the self member event isn't present
        // (e.g. the consumer didn't request m.room.member/$ME in required_state),
        // preserving the previous behavior.
        const selfUserId = client.getUserId();
        const selfMember = selfUserId
            ? room.currentState.getStateEvents(EventType.RoomMember, selfUserId)
            : null;
        const selfMembership = (selfMember?.getContent() as { membership?: string } | undefined)?.membership;
        room.updateMyMembership((selfMembership as Membership) ?? KnownMembership.Join);

        room.setMSC4186SummaryData(roomData.heroes, roomData.joined_count, roomData.invited_count);

        room.recalculate();
        if (roomData.initial && newToStore) {
            client.store.storeRoom(room);
            client.emit(ClientEvent.Room, room);
        }

        // check if any timeline events should bing and add them to the notifEvents array:
        // we'll purge this once we've fully processed the sync response
        this.addNotifications(timelineEvents);

        const processRoomEvent = async (e: MatrixEvent): Promise<void> => {
            client.emit(ClientEvent.Event, e);
            if (e.isState() && e.getType() == EventType.RoomEncryption && this.syncOpts.cryptoCallbacks) {
                await this.syncOpts.cryptoCallbacks.onCryptoEvent(room, e);
            }
        };

        await promiseMapSeries(stateEvents, processRoomEvent);
        await promiseMapSeries(timelineEvents, processRoomEvent);
        ephemeralEvents.forEach(function (e) {
            client.emit(ClientEvent.Event, e);
        });

        // Decrypt only the last message in all rooms to make sure we can generate a preview
        // And decrypt all events after the recorded read receipt to ensure an accurate
        // notification count
        room.decryptCriticalEvents();
    }

    /**
     * Injects events into a room's model.
     * @param stateEventList - A list of state events. This is the state
     * at the *END* of the timeline list if it is supplied.
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     * @param fromCache - whether these timeline events are a historical paint (cold catch-up
     * or cache replay) rather than live activity. Derived by the caller from the sync stream
     * (see processRoomData) — the server's num_live is ignored, since Continuwuity and other
     * non-Synapse servers leave it unset. Drives the RoomEvent.Timeline liveEvent flag.
     */
    public async injectRoomEvents(
        room: Room,
        stateEventList: MatrixEvent[],
        timelineEventList: MatrixEvent[] = [],
        fromCache: boolean = false,
    ): Promise<void> {
        // If there are no events in the timeline yet, initialise it with
        // the given state events
        const liveTimeline = room.getLiveTimeline();
        const timelineWasEmpty = liveTimeline.getEvents().length == 0;
        if (timelineWasEmpty) {
            // Passing these events into initialiseState will freeze them, so we need
            // to compute and cache the push actions for them now, otherwise sync dies
            // with an attempt to assign to read only property.
            // XXX: This is pretty horrible and is assuming all sorts of behaviour from
            // these functions that it shouldn't be. We should probably either store the
            // push actions cache elsewhere so we can freeze MatrixEvents, or otherwise
            // find some solution where MatrixEvents are immutable but allow for a cache
            // field.
            for (const ev of stateEventList) {
                this.client.getPushActionsForEvent(ev);
            }
            liveTimeline.initialiseState(stateEventList);
        }

        // If the timeline wasn't empty, we process the state events here: they're
        // defined as updates to the state before the start of the timeline, so this
        // starts to roll the state forward.
        // XXX: That's what we *should* do, but this can happen if we were previously
        // peeking in a room, in which case we obviously do *not* want to add the
        // state events here onto the end of the timeline. Historically, the js-sdk
        // has just set these new state events on the old and new state. This seems
        // very wrong because there could be events in the timeline that diverge the
        // state, in which case this is going to leave things out of sync. However,
        // for now I think it;s best to behave the same as the code has done previously.
        if (!timelineWasEmpty) {
            // XXX: As above, don't do this...
            //room.addLiveEvents(stateEventList || []);
            // Do this instead...
            room.oldState.setStateEvents(stateEventList);
            room.currentState.setStateEvents(stateEventList);
        }

        // These events are homogeneous: the caller routed scrollback out separately
        // (addEventsToTimeline, toStartOfTimeline=true) and dropped duplicates, so what's left
        // is either all live or all historical first-paint. One flag, one pass — fromCache drives
        // the RoomEvent.Timeline liveEvent flag (liveEvent = ...&& !fromCache) that consumers
        // gate unread/notify/sound/reorder on.
        await room.addLiveEvents(timelineEventList, { fromCache, addToState: false });

        room.recalculate();

        // resolve invites now we have set the latest state
        this.resolveInvites(room);
    }

    private resolveInvites(room: Room): void {
        if (!room || !this.opts.resolveInvitesToProfiles) {
            return;
        }
        const client = this.client;
        // For each invited room member we want to give them a displayname/avatar url
        // if they have one (the m.room.member invites don't contain this).
        room.getMembersWithMembership(KnownMembership.Invite).forEach(function (member) {
            if (member.requestedProfileInfo) return;
            member.requestedProfileInfo = true;
            // try to get a cached copy first.
            const user = client.getUser(member.userId);
            let promise: ReturnType<MatrixClient["getProfileInfo"]>;
            if (user) {
                promise = Promise.resolve({
                    avatar_url: user.avatarUrl,
                    displayname: user.displayName,
                });
            } else {
                promise = client.getProfileInfo(member.userId);
            }
            promise.then(
                function (info) {
                    // slightly naughty by doctoring the invite event but this means all
                    // the code paths remain the same between invite/join display name stuff
                    // which is a worthy trade-off for some minor pollution.
                    const inviteEvent = member.events.member!;
                    if (inviteEvent.getContent().membership !== KnownMembership.Invite) {
                        // between resolving and now they have since joined, so don't clobber
                        return;
                    }
                    inviteEvent.getContent().avatar_url = info.avatar_url;
                    inviteEvent.getContent().displayname = info.displayname;
                    // fire listeners
                    member.setMembershipEvent(inviteEvent, room.currentState);
                },
                function (_err) {
                    // OH WELL.
                },
            );
        });
    }

    public retryImmediately(): boolean {
        return true;
    }

    /**
     * Whether the given room has received a genuine LIVE sliding-sync response this session
     * (as opposed to only being painted from the boot cache). Consumers use this to gate
     * trust in a room's current data — see {@link liveSyncedRooms}.
     */
    public hasLiveSynced(roomId: string): boolean {
        return this.liveSyncedRooms.has(roomId);
    }

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
    private async rehydrateFromCache(): Promise<number> {
        let rooms: { roomId: string; data: MSC3575RoomData; receipt?: IMinimalEvent; accountData?: IMinimalEvent[] }[];
        try {
            rooms = await this.roomCache.loadAll();
        } catch (e) {
            this.syncOpts.logger.warn("[sss-cache] rehydrate load failed; cold start", e);
            return 0;
        }
        if (rooms.length === 0) return 0;
        this.syncOpts.logger.debug(`[sss-cache] rehydrating ${rooms.length} rooms from cache`);
        this.rehydrating = true;
        try {
            for (const { roomId, data, receipt, accountData } of rooms) {
                await this.onRoomData(roomId, data);
                // Replay our persisted read receipt AFTER the timeline exists, so the
                // read marker lands on an event we have and the unread count is right
                // on first paint instead of flickering from a stale cached count.
                if (receipt) {
                    try {
                        processEphemeralEvents(this.client, roomId, [receipt]);
                    } catch (e) {
                        this.syncOpts.logger.debug("[sss-cache] receipt replay failed", e);
                    }
                }
                // Replay per-room account_data (m.tag favourites, m.marked_unread, …)
                // so room.tags is populated on the cached paint — otherwise favourites
                // revert on reload until the live account_data extension re-delivers
                // them. addAccountData sets room.tags AND emits RoomEvent.Tags/AccountData,
                // so reactive consumers update too. The live response overwrites this.
                if (accountData?.length) {
                    try {
                        const room = this.client.getRoom(roomId);
                        if (room) room.addAccountData(mapEvents(this.client, roomId, accountData));
                    } catch (e) {
                        this.syncOpts.logger.debug("[sss-cache] account_data replay failed", e);
                    }
                }
            }
        } catch (e) {
            this.syncOpts.logger.warn("[sss-cache] rehydrate replay failed", e);
        } finally {
            this.rehydrating = false;
        }
        return rooms.length;
    }

    /**
     * Main entry point. Blocks until stop() is called.
     */
    public async sync(): Promise<void> {
        this.syncOpts.logger.debug("Sliding sync init loop");

        // Paint from the persistent per-room cache FIRST — before anything that
        // touches the network — so the UI has rooms/timelines/state immediately
        // (the sliding-sync equivalent of classic sync's getSavedSync() rehydrate).
        // This deliberately runs ahead of the push-rules fetch below: the rehydrate
        // is a purely local replay and does NOT need push rules to be correct. Room
        // unread COUNTS come from each cached room's server `unread_notifications`
        // (setUnreadNotificationCount), not from push evaluation; the only push
        // computation the replay triggers is on STATE events (addRoomEvents pre-
        // freezes their push actions to avoid a read-only crash), and state events
        // are never highlights, so a null ruleset there is a graceful no-op. Keeping
        // first paint off the network path is the whole point — a slow getPushRules()
        // round-trip used to gate the cached paint behind it. Strictly sequential:
        // no overlap, no race. Live initial=true responses dedupe against what we
        // replay here; best-effort, never fatal.
        const replayed = await this.rehydrateFromCache();
        // COUPLING INVARIANT: pos may only be resumed when the boot cache
        // actually restored the rooms behind it. Under a stateful connection
        // (conn_id) the server only sends DELTAS for rooms it believes we
        // hold — resuming a pos with rooms missing locally leaves them
        // invisible until something forces a reinitialise, because the server
        // never re-sends what it thinks we have. Dropping the pos forces
        // since=0: the server forgets the connection and re-sends everything as
        // initial. On a genuinely fresh login there is no pos, so it's a no-op.
        //
        // Two ways the invariant can break, and we must check BOTH — the
        // `replayed === 0` test alone only catches the total loss (cache wiped,
        // disabled, schema-bumped). A PARTIAL loss reads as a healthy boot:
        // hundreds of rooms replay, the pos resumes, and the handful the cache
        // dropped are silently absent. That was a routine occurrence while the
        // cap deleted records; now that over-cap rooms are shelled rather than
        // deleted (see SlidingSyncCache.prune), a genuine deletion is rare —
        // and this makes it cost one resync instead of missing rooms.
        if (replayed === 0 || this.roomCache.droppedRecords) {
            this.slidingSync.clearPersistedPos();
            await this.roomCache.clearDropped();
        }

        //   1) We need push rules so we can check if events should bing as we get them
        //      from the LIVE sync. This must complete before the live loop opens (below),
        //      but it no longer blocks first paint — the cache replay above already ran.
        while (!this.client.isGuest()) {
            try {
                this.syncOpts.logger.debug("Getting push rules...");
                const result = await this.client.getPushRules();
                this.syncOpts.logger.debug("Got push rules");
                this.client.pushRules = result;
                break;
            } catch (err) {
                this.syncOpts.logger.error("Getting push rules failed", err);
                if (this.shouldAbortSync(<MatrixError>err)) {
                    return;
                }
            }
        }

        // Seed the GLOBAL account data the room list is categorised by. Sliding
        // sync (Continuwuity) only resends global account_data that CHANGED since
        // `pos`, and on a restored pos it resends nothing — and unlike classic
        // sync we never rehydrate it from the store. So m.direct never arrives on
        // reload and the DM list comes up empty (likewise ignored users). Classic
        // sync rebuilds this from getSavedSync(); we have no equivalent, so fetch
        // the few global types we categorise by straight from the server.
        // Fire-and-forget: it must NOT block the sync loop (these fetches could
        // be slow). Each emits ClientEvent.AccountData when it lands, so the DM /
        // ignored-user lists update the moment the data arrives.
        void Promise.all(
            [EventType.Direct, EventType.IgnoredUserList, EventType.PushRules].map(async (type) => {
                try {
                    const content = await this.client.getAccountDataFromServer(type as never);
                    if (!content) return;
                    const [ev] = mapEvents(this.client, undefined, [{ type, content } as IMinimalEvent]);
                    if (!ev) return;
                    const prev = this.client.store.getAccountData(type);
                    this.client.store.storeAccountDataEvents([ev]);
                    this.client.emit(ClientEvent.AccountData, ev, prev);

                    // A DM's identity IS its people, so a DM room that sorts
                    // below the sliding window — and therefore never lands in
                    // the store — shows as a missing/empty entry even though
                    // m.direct maps it. Subscribe to every m.direct room so it
                    // materialises regardless of recency, the same way an
                    // opened room does. (Merge into the existing subscription
                    // set so app-driven subscriptions aren't clobbered.)
                    if (type === EventType.Direct && content && typeof content === "object") {
                        const dmRoomIds = new Set<string>();
                        for (const rooms of Object.values(content as unknown as Record<string, unknown>)) {
                            if (Array.isArray(rooms)) {
                                for (const r of rooms) if (typeof r === "string") dmRoomIds.add(r);
                            }
                        }
                        if (dmRoomIds.size) {
                            // Materialise DMs with a LEAN subscription, not the
                            // default heavy one (timeline_limit 50 + power/widget/
                            // emoji state). We only need a DM to APPEAR with a name
                            // — hauling 50 messages + heavy state for every DM on
                            // startup is real load weight. Opening a DM upgrades it
                            // (the consumer's on-open backfill fills the timeline).
                            this.slidingSync.addCustomSubscription(DM_MATERIALIZE_SUB, {
                                timeline_limit: 1,
                                required_state: DEFAULT_SLIDING_SYNC_REQUIRED_STATE,
                            });
                            const subs = this.slidingSync.getRoomSubscriptions();
                            let added = false;
                            for (const id of dmRoomIds) {
                                if (!subs.has(id)) {
                                    subs.add(id);
                                    this.slidingSync.useCustomSubscription(id, DM_MATERIALIZE_SUB);
                                    added = true;
                                }
                            }
                            if (added) this.slidingSync.modifyRoomSubscriptions(subs);
                        }
                    }
                } catch {
                    /* not set / unreachable — non-fatal */
                }
            }),
        );

        // start syncing — the dedicated encryption sync runs in parallel (its
        // start() is its own long-lived loop, so we don't await it), then the
        // room sync drives this call.
        this.encryptionSync?.start().catch((e) => this.syncOpts.logger.error("encryption sliding sync failed", e));
        await this.slidingSync.start();
    }

    /**
     * Stops the sync object from syncing.
     */
    public stop(): void {
        this.syncOpts.logger.debug("SyncApi.stop");
        this.slidingSync.stop();
        this.encryptionSync?.stop();
        // Flush any queued room writes so a quick reload still has the latest cache.
        void this.roomCache.stop();
    }

    /**
     * Sets the sync state and emits an event to say so
     * @param newState - The new state string
     * @param data - Object of additional data to emit in the event
     */
    private updateSyncState(newState: SyncState, data?: ISyncStateData): void {
        const old = this.syncState;
        this.syncState = newState;
        this.syncStateData = data;
        this.client.emit(ClientEvent.Sync, this.syncState, old, data);
    }

    /**
     * Takes a list of timelineEvents and adds and adds to notifEvents
     * as appropriate.
     * This must be called after the room the events belong to has been stored.
     *
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     */
    private addNotifications(timelineEventList: MatrixEvent[]): void {
        // gather our notifications into this.notifEvents
        if (!this.client.getNotifTimelineSet()) {
            return;
        }
        for (const timelineEvent of timelineEventList) {
            const pushActions = this.client.getPushActionsForEvent(timelineEvent);
            if (pushActions && pushActions.notify && pushActions.tweaks && pushActions.tweaks.highlight) {
                this.notifEvents.push(timelineEvent);
            }
        }
    }

    /**
     * Purge any events in the notifEvents array. Used after a /sync has been complete.
     * This should not be called at a per-room scope (e.g in onRoomData) because otherwise the ordering
     * will be messed up e.g room A gets a bing, room B gets a newer bing, but both in the same /sync
     * response. If we purge at a per-room scope then we could process room B before room A leading to
     * room B appearing earlier in the notifications timeline, even though it has the higher origin_server_ts.
     */
    private purgeNotifications(): void {
        this.notifEvents.sort(function (a, b) {
            return a.getTs() - b.getTs();
        });
        this.notifEvents.forEach((event) => {
            this.client.getNotifTimelineSet()?.addLiveEvent(event, { addToState: false });
        });
        this.notifEvents = [];
    }
}

function ensureNameEvent(client: MatrixClient, roomId: string, roomData: MSC3575RoomData): MSC3575RoomData {
    // make sure m.room.name is in required_state if there is a name, replacing anything previously
    // there if need be. This ensures clients transparently 'calculate' the right room name. Native
    // sliding sync clients should just read the "name" field.
    if (!roomData.name) {
        return roomData;
    }
    for (const stateEvent of roomData.required_state) {
        if (stateEvent.type === EventType.RoomName && stateEvent.state_key === "") {
            stateEvent.content = {
                name: roomData.name,
            };
            return roomData;
        }
    }
    // No existing m.room.name → fabricate one. For DMs / unnamed rooms the
    // server's computed `name` can read as "me, other + WhatsApp bot": it can
    // fold in ourselves and bridge bots. So fabricate the name the way a client
    // would compute it — from `heroes`, excluding functional (bridge-bot)
    // members — and only fall back to the server `name` when there is no hero
    // signal (so named rooms that omitted m.room.name still surface a name).
    const heroName = dmNameFromHeroes(roomData, client.getUserId());
    roomData.required_state.push({
        event_id: "$fake-sliding-sync-name-event-" + roomId,
        state_key: "",
        type: EventType.RoomName,
        content: {
            name: heroName ?? roomData.name,
        },
        sender: client.getUserId()!,
        origin_server_ts: new Date().getTime(),
    });
    return roomData;
}

/**
 * Compute a DM/group room name from the MSC4186 `heroes` summary the way a
 * client does: the other member(s)' display names, excluding functional
 * (bridge-bot) members listed in any io.element.functional_members state event
 * present in required_state. `heroes` already excludes the syncing user.
 * Returns undefined when there are no usable heroes.
 */
function dmNameFromHeroes(roomData: MSC3575RoomData, selfUserId?: string | null): string | undefined {
    const heroes = roomData.heroes;
    if (!Array.isArray(heroes) || heroes.length === 0) return undefined;

    const functional = new Set<string>();
    // Display names from any m.room.member events in required_state, so a hero
    // whose summary entry has no inline displayname (Continuwuity often omits it)
    // still resolves to a real name instead of a bare mxid.
    const memberNames = new Map<string, string>();
    for (const ev of roomData.required_state ?? []) {
        if (ev.type === UNSTABLE_ELEMENT_FUNCTIONAL_USERS.name && ev.state_key === "") {
            const svc = (ev.content as { service_members?: string[] })?.service_members;
            if (Array.isArray(svc)) svc.forEach((u) => functional.add(u));
        } else if (ev.type === EventType.RoomMember && typeof ev.state_key === "string") {
            const dn = (ev.content as { displayname?: string })?.displayname;
            if (dn) memberNames.set(ev.state_key, dn);
        }
    }

    // Exclude functional (bridge-bot) members AND the syncing user — a buggy
    // server can list us among the heroes, which used to fold "me" into the name.
    const usable = heroes.filter((h) => !functional.has(h.user_id) && h.user_id !== selfUserId);
    // Real name preference: hero's inline displayname → member-event displayname
    // → mxid only as a last resort (a contact whose profile we truly lack yet).
    const names = usable.map((h) => h.displayname || memberNames.get(h.user_id) || h.user_id);
    if (names.length === 0) return undefined;
    if (names.length === 1) return names[0];
    if (names.length === 2) return `${names[0]} and ${names[1]}`;
    return `${names[0]} and ${names.length - 1} others`;
}

type TaggedEvent = (IStrippedState | IRoomEvent | IStateEvent | IMinimalEvent) & { room_id?: string };

// Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts,
// just outside the class.
function mapEvents(client: MatrixClient, roomId: string | undefined, events: object[], decrypt = true): MatrixEvent[] {
    const mapper = client.getEventMapper({ decrypt });
    return (events as TaggedEvent[]).map(function (e) {
        e.room_id = roomId;
        return mapper(e);
    });
}

function processEphemeralEvents(client: MatrixClient, roomId: string, ephEvents: IMinimalEvent[]): void {
    const ephemeralEvents = mapEvents(client, roomId, ephEvents);
    const room = client.getRoom(roomId);
    if (!room) {
        // Expected under sliding sync: ephemeral (typing/receipts) can arrive
        // for rooms outside the current window. Not an error.
        logger.debug("got ephemeral events for room but room doesn't exist on client:", roomId);
        return;
    }
    room.addEphemeralEvents(ephemeralEvents);
    ephemeralEvents.forEach((e) => {
        client.emit(ClientEvent.Event, e);
    });
}
