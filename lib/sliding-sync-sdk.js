import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
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

import { NotificationCountType, Room, RoomEvent } from "./models/room.js";
import { logger } from "./logger.js";
import { promiseMapSeries } from "./utils.js";
import { EventTimeline } from "./models/event-timeline.js";
import { ClientEvent } from "./client.js";
import { SyncState, SyncApi, _createAndReEmitRoom, defaultClientOpts, defaultSyncApiOpts, processToDeviceMessages } from "./sync.js";
import { MatrixError } from "./http-api/index.js";
import { ExtensionState, SlidingSync, SlidingSyncEvent, SlidingSyncState, DEFAULT_SLIDING_SYNC_REQUIRED_STATE } from "./sliding-sync.js";
import { SlidingSyncCache } from "./sliding-sync-cache.js";
import { EventType, UNSTABLE_ELEMENT_FUNCTIONAL_USERS } from "./@types/event.js";
import { RoomStateEvent } from "./models/room-state.js";
import { RoomMemberEvent } from "./models/room-member.js";
import { KnownMembership } from "./@types/membership.js";

// Number of consecutive failed syncs that will lead to a syncState of ERROR as opposed
// to RECONNECTING. This is needed to inform the client of server issues when the
// keepAlive is successful but the server /sync fails.
var FAILED_SYNC_ERROR_THRESHOLD = 3;

/** Poll timeout for the dedicated encryption sync. The server long-polls on a
 * per-user watcher and WAKES on new data (to-device, device-list changes, OTK
 * counts are all watched), so delivery latency is wake-driven, not poll-driven
 * — a long timeout just cuts idle request volume. The server caps the hang at
 * 30s. (The old 3s poll dated from the disproven "the server never wakes"
 * model.) During a to-device handshake the boost mechanism in
 * {@link SlidingSync#start} still fast-polls as a belt-and-braces. */
var ENCRYPTION_SYNC_TIMEOUT_MS = 30000;

/** Name of the lean room subscription used to bulk-materialise DM rooms (just
 * enough state to show them in the list, NOT the heavy opened-room set). */
var DM_MATERIALIZE_SUB = "lean-materialize";
class ExtensionE2EE {
  constructor(crypto) {
    this.crypto = crypto;
  }
  name() {
    return "e2ee";
  }
  when() {
    return ExtensionState.PreProcess;
  }
  onRequest(isInitial) {
    var _this = this;
    return _asyncToGenerator(function* () {
      if (isInitial) {
        // In SSS, the `?pos=` contains the stream position for device list updates.
        // If we do not have a `?pos=` (e.g because we forgot it, or because the server
        // invalidated our connection) then we MUST invlaidate all device lists because
        // the server will not tell us the delta. This will then cause UTDs as we will fail
        // to encrypt for new devices. This is an expensive call, so we should
        // really really remember `?pos=` wherever possible.
        logger.log("ExtensionE2EE: invalidating all device lists due to missing 'pos'");
        yield _this.crypto.markAllTrackedUsersAsDirty();
      }
      return {
        enabled: true // this is sticky so only send it on the initial request
      };
    })();
  }
  onResponse(data) {
    var _this2 = this;
    return _asyncToGenerator(function* () {
      // Handle device list updates
      if (data.device_lists) {
        yield _this2.crypto.processDeviceLists(data.device_lists);
      }

      // Handle one_time_keys_count and unused_fallback_key_types
      yield _this2.crypto.processKeyCounts(data.device_one_time_keys_count, data["device_unused_fallback_key_types"] || data["org.matrix.msc2732.device_unused_fallback_key_types"]);

      // AWAIT: drain the outgoing-request pump before this cycle returns, so the next
      // (fast) encryption-sync poll's to-device receive does not overlap the pump on the
      // single non-reentrant OlmMachine — which corrupts in-flight SAS (spurious
      // m.mismatched_sas). See RustCrypto.onSyncCompleted.
      yield _this2.crypto.onSyncCompleted({});
    })();
  }
}
class ExtensionToDevice {
  constructor(client, cryptoCallbacks) {
    this.client = client;
    this.cryptoCallbacks = cryptoCallbacks;
    _defineProperty(this, "nextBatch", null);
  }
  name() {
    return "to_device";
  }
  when() {
    return ExtensionState.PreProcess;
  }
  onRequest(isInitial) {
    var _this3 = this;
    return _asyncToGenerator(function* () {
      return {
        since: _this3.nextBatch !== null ? _this3.nextBatch : undefined,
        limit: 100,
        enabled: true
      };
    })();
  }
  onResponse(data) {
    var _this4 = this;
    return _asyncToGenerator(function* () {
      var events = data["events"] || [];
      var receivedToDeviceMessages;
      if (_this4.cryptoCallbacks) {
        receivedToDeviceMessages = yield _this4.cryptoCallbacks.preprocessToDeviceMessages(events);
      } else {
        receivedToDeviceMessages = events.map(rawEvent => (
        // Crypto is not enabled, so we just return the events.
        {
          message: rawEvent,
          encryptionInfo: null
        }));
      }
      processToDeviceMessages(receivedToDeviceMessages, _this4.client);
      _this4.nextBatch = data.next_batch;
    })();
  }
}
class ExtensionAccountData {
  constructor(client) {
    this.client = client;
  }
  name() {
    return "account_data";
  }
  when() {
    return ExtensionState.PostProcess;
  }
  onRequest(isInitial) {
    return _asyncToGenerator(function* () {
      return {
        enabled: true
      };
    })();
  }
  onResponse(data) {
    var _this5 = this;
    return _asyncToGenerator(function* () {
      if (data.global && data.global.length > 0) {
        _this5.processGlobalAccountData(data.global);
      }
      for (var roomId in data.rooms) {
        var accountDataEvents = mapEvents(_this5.client, roomId, data.rooms[roomId]);
        var room = _this5.client.getRoom(roomId);
        if (!room) {
          // Expected under sliding sync: extensions can carry data for
          // rooms outside the current window. Not an error.
          logger.debug("got account data for room but room doesn't exist on client:", roomId);
          continue;
        }
        room.addAccountData(accountDataEvents);
        accountDataEvents.forEach(e => {
          _this5.client.emit(ClientEvent.Event, e);
        });
      }
    })();
  }
  processGlobalAccountData(globalAccountData) {
    var events = mapEvents(this.client, undefined, globalAccountData);
    var prevEventsMap = events.reduce((m, c) => {
      m[c.getType()] = this.client.store.getAccountData(c.getType());
      return m;
    }, {});
    this.client.store.storeAccountDataEvents(events);
    events.forEach(accountDataEvent => {
      // Honour push rules that come down the sync stream but also
      // honour push rules that were previously cached. Base rules
      // will be updated when we receive push rules via getPushRules
      // (see sync) before syncing over the network.
      if (accountDataEvent.getType() === EventType.PushRules) {
        var _prevEventsMap$EventT;
        var rules = accountDataEvent.getContent();
        // Only re-apply push rules when they actually changed. The
        // server can resend global account data on every sliding-sync
        // response, and re-running setPushRules each time is wasteful
        // (rewriteDefaultRules rebuilds the rule set) and floods logs
        // with "Missing/Adding default global ... push rule".
        var prevRules = (_prevEventsMap$EventT = prevEventsMap[EventType.PushRules]) === null || _prevEventsMap$EventT === void 0 ? void 0 : _prevEventsMap$EventT.getContent();
        if (!prevRules || JSON.stringify(prevRules) !== JSON.stringify(rules)) {
          this.client.setPushRules(rules);
        }
      }
      var prevEvent = prevEventsMap[accountDataEvent.getType()];
      this.client.emit(ClientEvent.AccountData, accountDataEvent, prevEvent);
      return accountDataEvent;
    });
  }
}
class ExtensionTyping {
  constructor(client) {
    this.client = client;
  }
  name() {
    return "typing";
  }
  when() {
    return ExtensionState.PostProcess;
  }
  onRequest(isInitial) {
    return _asyncToGenerator(function* () {
      return {
        enabled: true
      };
    })();
  }
  onResponse(data) {
    var _this6 = this;
    return _asyncToGenerator(function* () {
      if (!(data !== null && data !== void 0 && data.rooms)) {
        return;
      }
      for (var roomId in data.rooms) {
        processEphemeralEvents(_this6.client, roomId, [data.rooms[roomId]]);
      }
    })();
  }
}
class ExtensionReceipts {
  constructor(client) {
    this.client = client;
  }
  name() {
    return "receipts";
  }
  when() {
    return ExtensionState.PostProcess;
  }
  onRequest(isInitial) {
    return _asyncToGenerator(function* () {
      return {
        enabled: true
      };
    })();
  }
  onResponse(data) {
    var _this7 = this;
    return _asyncToGenerator(function* () {
      if (!(data !== null && data !== void 0 && data.rooms)) {
        return;
      }
      for (var roomId in data.rooms) {
        processEphemeralEvents(_this7.client, roomId, [data.rooms[roomId]]);
      }
    })();
  }
}

/**
 * A copy of SyncApi such that it can be used as a drop-in replacement for sync v2. For the actual
 * sliding sync API, see sliding-sync.ts or the class SlidingSync.
 */
export class SlidingSyncSdk {
  constructor(slidingSync, client, opts, syncOpts) {
    var _this$client$getUserI;
    this.slidingSync = slidingSync;
    this.client = client;
    _defineProperty(this, "opts", void 0);
    _defineProperty(this, "syncOpts", void 0);
    _defineProperty(this, "syncState", null);
    _defineProperty(this, "syncStateData", void 0);
    _defineProperty(this, "lastPos", null);
    _defineProperty(this, "failCount", 0);
    /** Dedicated fast-poll connection for to_device + e2ee (see constructor). */
    _defineProperty(this, "encryptionSync", void 0);
    /** Persistent per-room cache; replayed on boot so the UI paints before the network answers. */
    _defineProperty(this, "roomCache", void 0);
    _defineProperty(this, "notifEvents", []);
    // accumulator of sync events in the current sync response
    /** True while replaying cached rooms on boot, so onRoomData doesn't re-persist them. */
    _defineProperty(this, "rehydrating", false);
    /**
     * Rooms that have received a genuine LIVE sliding-sync response this session (NOT a
     * cache rehydrate). Monotonic — a room never leaves once it's in. Consumers use this
     * to decide whether a room's data (e.g. its unread count) is trustworthy-current vs a
     * possibly-stale cached value: sliding sync loads rooms partially/incrementally, so a
     * rehydrated-but-not-yet-live room's count must be treated as provisional.
     */
    _defineProperty(this, "liveSyncedRooms", new Set());
    /** Last wall-clock we requested a connection re-init for a delta targeting an unknown room. */
    _defineProperty(this, "lastUnknownRoomReinit", 0);
    /**
     * Lazily-created classic SyncApi used SOLELY for the isolated one-shot /sync that
     * fetches left rooms (see {@link syncLeftRooms}). It is never started (.sync() is
     * not called), so it runs no live loop and does not compete with the sliding-sync
     * transport — it only issues a single filtered request on demand.
     */
    _defineProperty(this, "leftRoomsSyncApi", void 0);
    this.opts = defaultClientOpts(opts);
    this.syncOpts = defaultSyncApiOpts(syncOpts);
    this.roomCache = new SlidingSyncCache((_this$client$getUserI = this.client.getUserId()) !== null && _this$client$getUserI !== void 0 ? _this$client$getUserI : undefined, this.syncOpts.logger);
    if (client.getNotifTimelineSet()) {
      client.reEmitter.reEmit(client.getNotifTimelineSet(), [RoomEvent.Timeline, RoomEvent.TimelineReset]);
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
    var mainExtensions = [new ExtensionAccountData(this.client), new ExtensionTyping(this.client), new ExtensionReceipts(this.client)];
    mainExtensions.forEach(ext => {
      this.slidingSync.registerExtension(ext);
    });

    // Dedicated ENCRYPTION sync: a SECOND connection (its own conn_id) with no
    // room lists, carrying only the to_device + e2ee extensions, polled fast.
    // This keeps latency-sensitive crypto — verification handshakes, room-key
    // shares, device-list updates — off the slow room long-poll, mirroring the
    // Rust SDK / Element X two-connection design. Without it, to-device sits
    // behind the room poll and "immediate" things (verification, UTD recovery)
    // lag by a whole poll cycle each step.
    this.encryptionSync = new SlidingSync(this.client.baseUrl, new Map(), {
      timeline_limit: 0,
      required_state: []
    }, this.client, ENCRYPTION_SYNC_TIMEOUT_MS, "encryption");
    var cryptoExtensions = [
    // to_device is delivered even without crypto (it just won't decrypt).
    new ExtensionToDevice(this.client, this.syncOpts.cryptoCallbacks)];
    if (this.syncOpts.cryptoCallbacks) {
      cryptoExtensions.push(new ExtensionE2EE(this.syncOpts.cryptoCallbacks));
    }
    cryptoExtensions.forEach(ext => {
      this.encryptionSync.registerExtension(ext);
    });
  }
  onRoomData(roomId, roomData) {
    var _this8 = this;
    return _asyncToGenerator(function* () {
      try {
        var room = _this8.client.store.getRoom(roomId);
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
            var now = Date.now();
            if (now - _this8.lastUnknownRoomReinit > 5 * 60 * 1000) {
              _this8.lastUnknownRoomReinit = now;
              _this8.syncOpts.logger.warn("Received a delta for unknown room ".concat(roomId, "; local state is behind the ") + "connection \u2014 reinitialising the sliding-sync connection");
              _this8.slidingSync.reinitialize();
            } else {
              _this8.syncOpts.logger.debug("delta for unknown room (reinit already requested recently), skipping", roomId);
            }
            return;
          }
          room = _createAndReEmitRoom(_this8.client, roomId, _this8.opts);
        }
        yield _this8.processRoomData(_this8.client, room, roomData);
        // Remember this room's data so the next boot can paint it before the
        // network answers. Skipped while replaying (the data came FROM cache).
        // The read-receipt snapshot below is now a SECONDARY nicety (it gives the
        // open room a correct marker on first paint); the primary stale-unread fix
        // is consumer-side confidence gating off `liveSyncedRooms` (see
        // hasLiveSynced / MatrixClient.isRoomLiveSynced).
        if (!_this8.rehydrating) {
          // This room now has verifiably-current data this session.
          _this8.liveSyncedRooms.add(roomId);
          var receipt;
          var uid = _this8.client.getUserId();
          // Source the marker from getEventReadUpTo — the SAME accessor the
          // unread UI uses. It returns the LATEST read position across public,
          // PRIVATE and synthetic receipts. getReadReceiptForUserId alone only
          // sees the public m.read type, so a user whose latest receipt is a
          // private one would get a STALE marker persisted — silently bringing
          // the flicker back for exactly those users. We replay it as a plain
          // unthreaded public receipt: type doesn't matter for unread, only that
          // it lands on the same (latest) event so the reloaded count matches.
          var readUpToId = uid ? room.getEventReadUpTo(uid, false) : null;
          if (uid && readUpToId) {
            var _findEventById$getTs, _findEventById;
            var ts = (_findEventById$getTs = (_findEventById = room.findEventById(readUpToId)) === null || _findEventById === void 0 ? void 0 : _findEventById.getTs()) !== null && _findEventById$getTs !== void 0 ? _findEventById$getTs : 0;
            receipt = {
              type: "m.receipt",
              content: {
                [readUpToId]: {
                  "m.read": {
                    [uid]: {
                      ts
                    }
                  }
                }
              }
            };
          }
          // Persist the room's per-room account_data (m.tag favourites,
          // m.marked_unread, …) too: it rides a separate extension and isn't in
          // MSC3575RoomData, so without this the rehydrated room loses room.tags
          // and favourites revert on reload until the live account_data arrives.
          var accountData = Array.from(room.accountData.values()).map(e => ({
            type: e.getType(),
            content: e.getContent()
          }));
          _this8.roomCache.put(roomId, roomData, receipt, accountData.length ? accountData : undefined);
        }
      } catch (e) {
        // Resilience: one malformed room must not break sliding sync (it
        // arrives per-room here, so an unhandled rejection would otherwise
        // surface as a spurious error and skip nothing useful).
        _this8.syncOpts.logger.error("Failed to process sliding-sync data for room ".concat(roomId, "; skipping"), e);
      }
    })();
  }
  onLifecycle(state, resp, err) {
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
            fromCache: false
          });
        }
        // Conversely, Element won't show the room list unless there is at least 1x SyncState.Syncing
        // so hence for the very first sync we will fire prepared then immediately syncing.
        this.updateSyncState(SyncState.Syncing, {
          oldSyncToken: this.lastPos,
          nextSyncToken: resp.pos,
          catchingUp: false,
          fromCache: false
        });
        this.lastPos = resp.pos;
        break;
      case SlidingSyncState.RequestFinished:
        if (err) {
          this.failCount += 1;
          this.updateSyncState(this.failCount > FAILED_SYNC_ERROR_THRESHOLD ? SyncState.Error : SyncState.Reconnecting, {
            error: new MatrixError(err)
          });
          if (this.shouldAbortSync(new MatrixError(err))) {
            return; // shouldAbortSync actually stops syncing too so we don't need to do anything.
          }
        } else {
          this.failCount = 0;
          this.syncOpts.logger.debug("SlidingSyncState.RequestFinished with ".concat(Object.keys((resp === null || resp === void 0 ? void 0 : resp.rooms) || []).length, " rooms"));
        }
        break;
    }
  }
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
  syncLeftRooms() {
    var _this9 = this;
    return _asyncToGenerator(function* () {
      if (!_this9.leftRoomsSyncApi) {
        _this9.leftRoomsSyncApi = new SyncApi(_this9.client, _this9.opts, _this9.syncOpts);
      }
      return _this9.leftRoomsSyncApi.syncLeftRooms();
    })();
  }

  /**
   * Peek into a room. This will result in the room in question being synced so it
   * is accessible via getRooms(). Live updates for the room will be provided.
   * @param roomId - The room ID to peek into.
   * @returns A promise which resolves once the room has been added to the
   * store.
   */
  peek(roomId) {
    return _asyncToGenerator(function* () {
      return null; // TODO
    })();
  }

  /**
   * Stop polling for updates in the peeked room. NOPs if there is no room being
   * peeked.
   */
  stopPeeking() {
    // TODO
  }

  /**
   * Specify the set_presence value to be used for subsequent calls to the Sync API.
   * @param presence - the presence to specify to set_presence of sync calls
   */
  setPresence(presence) {
    // TODO not possible in sliding sync yet
  }

  /**
   * Returns the current state of this sync object
   * @see MatrixClient#event:"sync"
   */
  getSyncState() {
    return this.syncState;
  }

  /**
   * Returns the additional data object associated with
   * the current sync state, or null if there is no
   * such data.
   * Sync errors, if available, are put in the 'error' key of
   * this object.
   */
  getSyncStateData() {
    var _this$syncStateData;
    return (_this$syncStateData = this.syncStateData) !== null && _this$syncStateData !== void 0 ? _this$syncStateData : null;
  }

  // Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts

  createRoom(roomId) {
    // XXX cargoculted from sync.ts
    var {
      timelineSupport
    } = this.client;
    var room = new Room(roomId, this.client, this.client.getUserId(), {
      lazyLoadMembers: this.opts.lazyLoadMembers,
      pendingEventOrdering: this.opts.pendingEventOrdering,
      timelineSupport
    });
    this.client.reEmitter.reEmit(room, [RoomEvent.Name, RoomEvent.Redaction, RoomEvent.RedactionCancelled, RoomEvent.Receipt, RoomEvent.Tags, RoomEvent.LocalEchoUpdated, RoomEvent.AccountData, RoomEvent.MyMembership, RoomEvent.Timeline, RoomEvent.TimelineReset, RoomEvent.UnreadNotifications]);
    this.registerStateListeners(room);
    return room;
  }
  registerStateListeners(room) {
    // XXX cargoculted from sync.ts
    // we need to also re-emit room state and room member events, so hook it up
    // to the client now. We need to add a listener for RoomState.members in
    // order to hook them correctly.
    this.client.reEmitter.reEmit(room.currentState, [RoomStateEvent.Events, RoomStateEvent.Members, RoomStateEvent.NewMember, RoomStateEvent.Update]);
    room.currentState.on(RoomStateEvent.NewMember, (event, state, member) => {
      var _this$client$getUser;
      member.user = (_this$client$getUser = this.client.getUser(member.userId)) !== null && _this$client$getUser !== void 0 ? _this$client$getUser : undefined;
      this.client.reEmitter.reEmit(member, [RoomMemberEvent.Name, RoomMemberEvent.Typing, RoomMemberEvent.PowerLevel, RoomMemberEvent.Membership]);
    });
  }

  /*
  private deregisterStateListeners(room: Room): void { // XXX cargoculted from sync.ts
      // could do with a better way of achieving this.
      room.currentState.removeAllListeners(RoomStateEvent.Events);
      room.currentState.removeAllListeners(RoomStateEvent.Members);
      room.currentState.removeAllListeners(RoomStateEvent.NewMember);
  } */

  shouldAbortSync(error) {
    if (error.errcode === "M_UNKNOWN_TOKEN") {
      // The logout already happened, we just need to stop.
      this.syncOpts.logger.warn("Token no longer valid - assuming logout");
      this.stop();
      this.updateSyncState(SyncState.Error, {
        error
      });
      return true;
    }
    return false;
  }
  processRoomData(client, room, roomData) {
    var _this0 = this;
    return _asyncToGenerator(function* () {
      var _selfMember$getConten, _ref2;
      // Only store the room the first time we see it. The server re-sends rooms
      // with initial=true whenever they (re-)enter a sliding window (e.g. as
      // the range grows), and store.storeRoom() registers a fresh
      // RoomState.members listener each call — so re-storing leaks listeners
      // (MaxListenersExceededWarning) on large accounts. State still updates
      // via injectRoomEvents regardless.
      var newToStore = !client.store.getRoom(room.roomId);
      roomData = ensureNameEvent(client, room.roomId, roomData);
      var stateEvents = mapEvents(_this0.client, room.roomId, roomData.required_state);
      // Prevent events from being decrypted ahead of time
      // this helps large account to speed up faster
      // room::decryptCriticalEvent is in charge of decrypting all the events
      // required for a client to function properly
      var timelineEvents = mapEvents(_this0.client, room.roomId, roomData.timeline, false);
      var ephemeralEvents = []; // TODO this.mapSyncEventsFormat(joinObj.ephemeral);

      // TODO: handle threaded / beacon events

      // Bucket the received window against what we already hold. Computed
      // BEFORE any timeline mutation below.
      var liveTimelineEvents = room.getLiveTimeline().getEvents();
      var hadEvents = liveTimelineEvents.length > 0;
      var didReset = false;
      if (roomData.limited || roomData.initial) {
        // we should not know about any of these timeline entries if this is a genuinely new room.
        // If we do, then we've effectively done scrollback (e.g requesting timeline_limit: 1 for
        // this room, then timeline_limit: 50).
        var knownEvents = new Set();
        liveTimelineEvents.forEach(e => {
          knownEvents.add(e.getId());
        });
        var anyKnown = timelineEvents.some(e => knownEvents.has(e.getId()));
        if (roomData.limited && hadEvents && !anyKnown) {
          var _roomData$prev_batch;
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
          room.resetLiveTimeline((_roomData$prev_batch = roomData.prev_batch) !== null && _roomData$prev_batch !== void 0 ? _roomData$prev_batch : null, null);
          // A gap means incremental notif tracking is broken; same as sync.ts.
          _this0.client.resetNotifTimelineSet();
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
          var oldEvents = [];
          var newEvents = [];
          var seenKnownEvent = false;
          for (var recvEvent of timelineEvents) {
            // oldest -> newest
            if (knownEvents.has(recvEvent.getId())) {
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
      var encrypted = room.hasEncryptionStateEvent();
      // we do this first so it's correct when any of the events fire
      if (roomData.notification_count != null) {
        room.setUnreadNotificationCount(NotificationCountType.Total, roomData.notification_count);
      }
      if (roomData.highlight_count != null) {
        // We track unread notifications ourselves in encrypted rooms, so don't
        // bother setting it here. We trust our calculations better than the
        // server's for this case, and therefore will assume that our non-zero
        // count is accurate.
        if (!encrypted || encrypted && room.getUnreadNotificationCount(NotificationCountType.Highlight) <= 0) {
          room.setUnreadNotificationCount(NotificationCountType.Highlight, roomData.highlight_count);
        }
      }
      if (roomData.bump_stamp) {
        room.setBumpStamp(roomData.bump_stamp);
      }
      if (Number.isInteger(roomData.invited_count)) {
        room.currentState.setInvitedMemberCount(roomData.invited_count);
      }
      if (Number.isInteger(roomData.joined_count)) {
        room.currentState.setJoinedMemberCount(roomData.joined_count);
        // UTD guard: if this is an encrypted room whose full member roster we
        // already loaded, but the server now reports MORE joined members than
        // we actually hold, a user joined whose m.room.member event the lean
        // required_state never delivered. Our cached roster is stale, so the
        // next encrypt would share the megolm key to an INCOMPLETE recipient
        // set and the missing member(s) UTD on every device. Re-arm member
        // loading (Rust's mark_members_missing analogue) so the next key
        // share re-fetches /members and includes them.
        if (encrypted && room.membersLoaded() && roomData.joined_count > room.getJoinedMembers().length) {
          room.invalidateLoadedMembers();
        }
      }
      if (roomData.invite_state) {
        var _inviteSelfMember$get, _ref;
        var inviteStateEvents = mapEvents(_this0.client, room.roomId, roomData.invite_state);
        yield _this0.injectRoomEvents(room, inviteStateEvents);
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
        var inviteSelfId = _this0.client.getUserId();
        var inviteSelfMember = inviteSelfId ? room.currentState.getStateEvents(EventType.RoomMember, inviteSelfId) : null;
        var strippedMembership = inviteSelfMember === null || inviteSelfMember === void 0 || (_inviteSelfMember$get = inviteSelfMember.getContent()) === null || _inviteSelfMember$get === void 0 ? void 0 : _inviteSelfMember$get.membership;
        room.updateMyMembership((_ref = strippedMembership) !== null && _ref !== void 0 ? _ref : KnownMembership.Invite);
        if (roomData.initial && newToStore) {
          room.recalculate();
          _this0.client.store.storeRoom(room);
          _this0.client.emit(ClientEvent.Room, room);
        }
        inviteStateEvents.forEach(e => {
          _this0.client.emit(ClientEvent.Event, e);
        });
        return;
      }
      if (roomData.limited && !didReset && !hadEvents) {
        var _roomData$prev_batch2;
        // First paint of a fresh timeline: set the back-pagination token
        // *before* adding any events so clients can start back-paginating.
        // Only then — a reset already set the fresh timeline's token, and
        // when we HOLD older events the timeline's existing token is the
        // right deeper-history continuation; overwriting it with this
        // window's prev_batch (which points just before the window, i.e.
        // AHEAD of our timeline start) would make back-pagination re-fetch
        // events we already hold and skip the genuinely older history.
        room.getLiveTimeline().setPaginationToken((_roomData$prev_batch2 = roomData.prev_batch) !== null && _roomData$prev_batch2 !== void 0 ? _roomData$prev_batch2 : null, EventTimeline.BACKWARDS);
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
      var firstPaint = !_this0.liveSyncedRooms.has(room.roomId);
      yield _this0.injectRoomEvents(room, stateEvents, timelineEvents, firstPaint);

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
      var selfUserId = client.getUserId();
      var selfMember = selfUserId ? room.currentState.getStateEvents(EventType.RoomMember, selfUserId) : null;
      var selfMembership = selfMember === null || selfMember === void 0 || (_selfMember$getConten = selfMember.getContent()) === null || _selfMember$getConten === void 0 ? void 0 : _selfMember$getConten.membership;
      room.updateMyMembership((_ref2 = selfMembership) !== null && _ref2 !== void 0 ? _ref2 : KnownMembership.Join);
      room.setMSC4186SummaryData(roomData.heroes, roomData.joined_count, roomData.invited_count);
      room.recalculate();
      if (roomData.initial && newToStore) {
        client.store.storeRoom(room);
        client.emit(ClientEvent.Room, room);
      }

      // check if any timeline events should bing and add them to the notifEvents array:
      // we'll purge this once we've fully processed the sync response
      _this0.addNotifications(timelineEvents);
      var processRoomEvent = /*#__PURE__*/function () {
        var _ref3 = _asyncToGenerator(function* (e) {
          client.emit(ClientEvent.Event, e);
          if (e.isState() && e.getType() == EventType.RoomEncryption && _this0.syncOpts.cryptoCallbacks) {
            yield _this0.syncOpts.cryptoCallbacks.onCryptoEvent(room, e);
          }
        });
        return function processRoomEvent(_x) {
          return _ref3.apply(this, arguments);
        };
      }();
      yield promiseMapSeries(stateEvents, processRoomEvent);
      yield promiseMapSeries(timelineEvents, processRoomEvent);
      ephemeralEvents.forEach(function (e) {
        client.emit(ClientEvent.Event, e);
      });

      // Decrypt only the last message in all rooms to make sure we can generate a preview
      // And decrypt all events after the recorded read receipt to ensure an accurate
      // notification count
      room.decryptCriticalEvents();
    })();
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
  injectRoomEvents(room, stateEventList) {
    var _arguments = arguments,
      _this1 = this;
    return _asyncToGenerator(function* () {
      var timelineEventList = _arguments.length > 2 && _arguments[2] !== undefined ? _arguments[2] : [];
      var fromCache = _arguments.length > 3 && _arguments[3] !== undefined ? _arguments[3] : false;
      // If there are no events in the timeline yet, initialise it with
      // the given state events
      var liveTimeline = room.getLiveTimeline();
      var timelineWasEmpty = liveTimeline.getEvents().length == 0;
      if (timelineWasEmpty) {
        // Passing these events into initialiseState will freeze them, so we need
        // to compute and cache the push actions for them now, otherwise sync dies
        // with an attempt to assign to read only property.
        // XXX: This is pretty horrible and is assuming all sorts of behaviour from
        // these functions that it shouldn't be. We should probably either store the
        // push actions cache elsewhere so we can freeze MatrixEvents, or otherwise
        // find some solution where MatrixEvents are immutable but allow for a cache
        // field.
        for (var ev of stateEventList) {
          _this1.client.getPushActionsForEvent(ev);
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
      yield room.addLiveEvents(timelineEventList, {
        fromCache,
        addToState: false
      });
      room.recalculate();

      // resolve invites now we have set the latest state
      _this1.resolveInvites(room);
    })();
  }
  resolveInvites(room) {
    if (!room || !this.opts.resolveInvitesToProfiles) {
      return;
    }
    var client = this.client;
    // For each invited room member we want to give them a displayname/avatar url
    // if they have one (the m.room.member invites don't contain this).
    room.getMembersWithMembership(KnownMembership.Invite).forEach(function (member) {
      if (member.requestedProfileInfo) return;
      member.requestedProfileInfo = true;
      // try to get a cached copy first.
      var user = client.getUser(member.userId);
      var promise;
      if (user) {
        promise = Promise.resolve({
          avatar_url: user.avatarUrl,
          displayname: user.displayName
        });
      } else {
        promise = client.getProfileInfo(member.userId);
      }
      promise.then(function (info) {
        // slightly naughty by doctoring the invite event but this means all
        // the code paths remain the same between invite/join display name stuff
        // which is a worthy trade-off for some minor pollution.
        var inviteEvent = member.events.member;
        if (inviteEvent.getContent().membership !== KnownMembership.Invite) {
          // between resolving and now they have since joined, so don't clobber
          return;
        }
        inviteEvent.getContent().avatar_url = info.avatar_url;
        inviteEvent.getContent().displayname = info.displayname;
        // fire listeners
        member.setMembershipEvent(inviteEvent, room.currentState);
      }, function (_err) {
        // OH WELL.
      });
    });
  }
  retryImmediately() {
    return true;
  }

  /**
   * Whether the given room has received a genuine LIVE sliding-sync response this session
   * (as opposed to only being painted from the boot cache). Consumers use this to gate
   * trust in a room's current data — see {@link liveSyncedRooms}.
   */
  hasLiveSynced(roomId) {
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
  rehydrateFromCache() {
    var _this10 = this;
    return _asyncToGenerator(function* () {
      var rooms;
      try {
        rooms = yield _this10.roomCache.loadAll();
      } catch (e) {
        _this10.syncOpts.logger.warn("[sss-cache] rehydrate load failed; cold start", e);
        return 0;
      }
      if (rooms.length === 0) return 0;
      _this10.syncOpts.logger.debug("[sss-cache] rehydrating ".concat(rooms.length, " rooms from cache"));
      _this10.rehydrating = true;
      try {
        for (var {
          roomId,
          data,
          receipt,
          accountData
        } of rooms) {
          yield _this10.onRoomData(roomId, data);
          // Replay our persisted read receipt AFTER the timeline exists, so the
          // read marker lands on an event we have and the unread count is right
          // on first paint instead of flickering from a stale cached count.
          if (receipt) {
            try {
              processEphemeralEvents(_this10.client, roomId, [receipt]);
            } catch (e) {
              _this10.syncOpts.logger.debug("[sss-cache] receipt replay failed", e);
            }
          }
          // Replay per-room account_data (m.tag favourites, m.marked_unread, …)
          // so room.tags is populated on the cached paint — otherwise favourites
          // revert on reload until the live account_data extension re-delivers
          // them. addAccountData sets room.tags AND emits RoomEvent.Tags/AccountData,
          // so reactive consumers update too. The live response overwrites this.
          if (accountData !== null && accountData !== void 0 && accountData.length) {
            try {
              var room = _this10.client.getRoom(roomId);
              if (room) room.addAccountData(mapEvents(_this10.client, roomId, accountData));
            } catch (e) {
              _this10.syncOpts.logger.debug("[sss-cache] account_data replay failed", e);
            }
          }
        }
      } catch (e) {
        _this10.syncOpts.logger.warn("[sss-cache] rehydrate replay failed", e);
      } finally {
        _this10.rehydrating = false;
      }
      return rooms.length;
    })();
  }

  /**
   * Main entry point. Blocks until stop() is called.
   */
  sync() {
    var _this11 = this;
    return _asyncToGenerator(function* () {
      var _this11$encryptionSyn;
      _this11.syncOpts.logger.debug("Sliding sync init loop");

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
      var replayed = yield _this11.rehydrateFromCache();
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
      if (replayed === 0 || _this11.roomCache.droppedRecords) {
        _this11.slidingSync.clearPersistedPos();
        yield _this11.roomCache.clearDropped();
      }

      //   1) We need push rules so we can check if events should bing as we get them
      //      from the LIVE sync. This must complete before the live loop opens (below),
      //      but it no longer blocks first paint — the cache replay above already ran.
      while (!_this11.client.isGuest()) {
        try {
          _this11.syncOpts.logger.debug("Getting push rules...");
          var result = yield _this11.client.getPushRules();
          _this11.syncOpts.logger.debug("Got push rules");
          _this11.client.pushRules = result;
          break;
        } catch (err) {
          _this11.syncOpts.logger.error("Getting push rules failed", err);
          if (_this11.shouldAbortSync(err)) {
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
      void Promise.all([EventType.Direct, EventType.IgnoredUserList, EventType.PushRules].map(/*#__PURE__*/function () {
        var _ref4 = _asyncToGenerator(function* (type) {
          try {
            var content = yield _this11.client.getAccountDataFromServer(type);
            if (!content) return;
            var [ev] = mapEvents(_this11.client, undefined, [{
              type,
              content
            }]);
            if (!ev) return;
            var prev = _this11.client.store.getAccountData(type);
            _this11.client.store.storeAccountDataEvents([ev]);
            _this11.client.emit(ClientEvent.AccountData, ev, prev);

            // A DM's identity IS its people, so a DM room that sorts
            // below the sliding window — and therefore never lands in
            // the store — shows as a missing/empty entry even though
            // m.direct maps it. Subscribe to every m.direct room so it
            // materialises regardless of recency, the same way an
            // opened room does. (Merge into the existing subscription
            // set so app-driven subscriptions aren't clobbered.)
            if (type === EventType.Direct && content && typeof content === "object") {
              var dmRoomIds = new Set();
              for (var rooms of Object.values(content)) {
                if (Array.isArray(rooms)) {
                  for (var r of rooms) if (typeof r === "string") dmRoomIds.add(r);
                }
              }
              if (dmRoomIds.size) {
                // Materialise DMs with a LEAN subscription, not the
                // default heavy one (timeline_limit 50 + power/widget/
                // emoji state). We only need a DM to APPEAR with a name
                // — hauling 50 messages + heavy state for every DM on
                // startup is real load weight. Opening a DM upgrades it
                // (the consumer's on-open backfill fills the timeline).
                _this11.slidingSync.addCustomSubscription(DM_MATERIALIZE_SUB, {
                  timeline_limit: 1,
                  required_state: DEFAULT_SLIDING_SYNC_REQUIRED_STATE
                });
                var subs = _this11.slidingSync.getRoomSubscriptions();
                var added = false;
                for (var id of dmRoomIds) {
                  if (!subs.has(id)) {
                    subs.add(id);
                    _this11.slidingSync.useCustomSubscription(id, DM_MATERIALIZE_SUB);
                    added = true;
                  }
                }
                if (added) _this11.slidingSync.modifyRoomSubscriptions(subs);
              }
            }
          } catch (_unused) {
            /* not set / unreachable — non-fatal */
          }
        });
        return function (_x2) {
          return _ref4.apply(this, arguments);
        };
      }()));

      // start syncing — the dedicated encryption sync runs in parallel (its
      // start() is its own long-lived loop, so we don't await it), then the
      // room sync drives this call.
      (_this11$encryptionSyn = _this11.encryptionSync) === null || _this11$encryptionSyn === void 0 || _this11$encryptionSyn.start().catch(e => _this11.syncOpts.logger.error("encryption sliding sync failed", e));
      yield _this11.slidingSync.start();
    })();
  }

  /**
   * Stops the sync object from syncing.
   */
  stop() {
    var _this$encryptionSync;
    this.syncOpts.logger.debug("SyncApi.stop");
    this.slidingSync.stop();
    (_this$encryptionSync = this.encryptionSync) === null || _this$encryptionSync === void 0 || _this$encryptionSync.stop();
    // Flush any queued room writes so a quick reload still has the latest cache.
    void this.roomCache.stop();
  }

  /**
   * Sets the sync state and emits an event to say so
   * @param newState - The new state string
   * @param data - Object of additional data to emit in the event
   */
  updateSyncState(newState, data) {
    var old = this.syncState;
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
  addNotifications(timelineEventList) {
    // gather our notifications into this.notifEvents
    if (!this.client.getNotifTimelineSet()) {
      return;
    }
    for (var timelineEvent of timelineEventList) {
      var pushActions = this.client.getPushActionsForEvent(timelineEvent);
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
  purgeNotifications() {
    this.notifEvents.sort(function (a, b) {
      return a.getTs() - b.getTs();
    });
    this.notifEvents.forEach(event => {
      var _this$client$getNotif;
      (_this$client$getNotif = this.client.getNotifTimelineSet()) === null || _this$client$getNotif === void 0 || _this$client$getNotif.addLiveEvent(event, {
        addToState: false
      });
    });
    this.notifEvents = [];
  }
}
function ensureNameEvent(client, roomId, roomData) {
  // make sure m.room.name is in required_state if there is a name, replacing anything previously
  // there if need be. This ensures clients transparently 'calculate' the right room name. Native
  // sliding sync clients should just read the "name" field.
  if (!roomData.name) {
    return roomData;
  }
  for (var stateEvent of roomData.required_state) {
    if (stateEvent.type === EventType.RoomName && stateEvent.state_key === "") {
      stateEvent.content = {
        name: roomData.name
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
  var heroName = dmNameFromHeroes(roomData, client.getUserId());
  roomData.required_state.push({
    event_id: "$fake-sliding-sync-name-event-" + roomId,
    state_key: "",
    type: EventType.RoomName,
    content: {
      name: heroName !== null && heroName !== void 0 ? heroName : roomData.name
    },
    sender: client.getUserId(),
    origin_server_ts: new Date().getTime()
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
function dmNameFromHeroes(roomData, selfUserId) {
  var heroes = roomData.heroes;
  if (!Array.isArray(heroes) || heroes.length === 0) return undefined;
  var functional = new Set();
  // Display names from any m.room.member events in required_state, so a hero
  // whose summary entry has no inline displayname (Continuwuity often omits it)
  // still resolves to a real name instead of a bare mxid.
  var memberNames = new Map();
  for (var ev of (_roomData$required_st = roomData.required_state) !== null && _roomData$required_st !== void 0 ? _roomData$required_st : []) {
    var _roomData$required_st;
    if (ev.type === UNSTABLE_ELEMENT_FUNCTIONAL_USERS.name && ev.state_key === "") {
      var _ev$content;
      var svc = (_ev$content = ev.content) === null || _ev$content === void 0 ? void 0 : _ev$content.service_members;
      if (Array.isArray(svc)) svc.forEach(u => functional.add(u));
    } else if (ev.type === EventType.RoomMember && typeof ev.state_key === "string") {
      var _ev$content2;
      var dn = (_ev$content2 = ev.content) === null || _ev$content2 === void 0 ? void 0 : _ev$content2.displayname;
      if (dn) memberNames.set(ev.state_key, dn);
    }
  }

  // Exclude functional (bridge-bot) members AND the syncing user — a buggy
  // server can list us among the heroes, which used to fold "me" into the name.
  var usable = heroes.filter(h => !functional.has(h.user_id) && h.user_id !== selfUserId);
  // Real name preference: hero's inline displayname → member-event displayname
  // → mxid only as a last resort (a contact whose profile we truly lack yet).
  var names = usable.map(h => h.displayname || memberNames.get(h.user_id) || h.user_id);
  if (names.length === 0) return undefined;
  if (names.length === 1) return names[0];
  if (names.length === 2) return "".concat(names[0], " and ").concat(names[1]);
  return "".concat(names[0], " and ").concat(names.length - 1, " others");
}
// Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts,
// just outside the class.
function mapEvents(client, roomId, events) {
  var decrypt = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : true;
  var mapper = client.getEventMapper({
    decrypt
  });
  return events.map(function (e) {
    e.room_id = roomId;
    return mapper(e);
  });
}
function processEphemeralEvents(client, roomId, ephEvents) {
  var ephemeralEvents = mapEvents(client, roomId, ephEvents);
  var room = client.getRoom(roomId);
  if (!room) {
    // Expected under sliding sync: ephemeral (typing/receipts) can arrive
    // for rooms outside the current window. Not an error.
    logger.debug("got ephemeral events for room but room doesn't exist on client:", roomId);
    return;
  }
  room.addEphemeralEvents(ephemeralEvents);
  ephemeralEvents.forEach(e => {
    client.emit(ClientEvent.Event, e);
  });
}
//# sourceMappingURL=sliding-sync-sdk.js.map