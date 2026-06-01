import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
/*
Copyright 2022-2024 The Matrix.org Foundation C.I.C.

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

import { logger } from "./logger.js";
import { TypedEventEmitter } from "./models/typed-event-emitter.js";
import { sleep } from "./utils.js";
// /sync requests allow you to set a timeout= but the request may continue
// beyond that and wedge forever, so we need to track how long we are willing
// to keep open the connection. This constant is *ADDED* to the timeout= value
// to determine the max time we're willing to wait.
var BUFFER_PERIOD_MS = 10 * 1000;
export var MSC3575_WILDCARD = "*";
export var MSC3575_STATE_KEY_ME = "$ME";
export var MSC3575_STATE_KEY_LAZY = "$LAZY";

/**
 * Represents a subscription to a room or set of rooms. Controls which events are returned.
 */

/**
 * Controls which rooms are returned in a given list.
 */

/**
 * Represents a list subscription.
 */

/**
 * A complete Sliding Sync request.
 */

/**
 * New format of hero introduced in MSC4186 with display name and avatar URL
 * in addition to just user_id (as it is on the wire, with underscores)
 * as opposed to Hero in room-summary.ts which has fields in camelCase
 * (and also a flag to note what format the hero came from).
 */

/**
 * A complete Sliding Sync response
 */

export var SlidingSyncState = /*#__PURE__*/function (SlidingSyncState) {
  /**
   * Fired by SlidingSyncEvent.Lifecycle event immediately before processing the response.
   */
  SlidingSyncState["RequestFinished"] = "FINISHED";
  /**
   * Fired by SlidingSyncEvent.Lifecycle event immediately after all room data listeners have been
   * invoked, but before list listeners.
   */
  SlidingSyncState["Complete"] = "COMPLETE";
  return SlidingSyncState;
}({});

/**
 * Internal Class. SlidingList represents a single list in sliding sync. The list can have filters,
 * multiple sliding windows, and maintains the index-\>room_id mapping.
 */
class SlidingList {
  /**
   * Construct a new sliding list.
   * @param list - The range, sort and filter values to use for this list.
   */
  constructor(list) {
    _defineProperty(this, "list", void 0);
    _defineProperty(this, "isModified", void 0);
    // returned data
    _defineProperty(this, "joinedCount", 0);
    this.replaceList(list);
  }

  /**
   * Mark this list as modified or not. Modified lists will return sticky params with calls to getList.
   * This is useful for the first time the list is sent, or if the list has changed in some way.
   * @param modified - True to mark this list as modified so all sticky parameters will be re-sent.
   */
  setModified(modified) {
    this.isModified = modified;
  }

  /**
   * Update the list range for this list. Does not affect modified status as list ranges are non-sticky.
   * @param newRanges - The new ranges for the list
   */
  updateListRange(newRanges) {
    this.list.ranges = JSON.parse(JSON.stringify(newRanges));
  }

  /**
   * Replace list parameters. All fields will be replaced with the new list parameters.
   * @param list - The new list parameters
   */
  replaceList(list) {
    var _list$filters, _list$ranges;
    list.filters = (_list$filters = list.filters) !== null && _list$filters !== void 0 ? _list$filters : {};
    list.ranges = (_list$ranges = list.ranges) !== null && _list$ranges !== void 0 ? _list$ranges : [];
    this.list = JSON.parse(JSON.stringify(list));
    this.isModified = true;

    // reset values as the join count may be very different (if filters changed) including the rooms
    // (e.g. sort orders or sliding window ranges changed)

    // the total number of joined rooms according to the server, always >= len(roomIndexToRoomId)
    this.joinedCount = 0;
  }

  /**
   * Return a copy of the list suitable for a request body.
   * @param forceIncludeAllParams - True to forcibly include all params even if the list
   * hasn't been modified. Callers may want to do this if they are modifying the list prior to calling
   * updateList.
   */
  getList(forceIncludeAllParams) {
    var list = {
      ranges: JSON.parse(JSON.stringify(this.list.ranges))
    };
    if (this.isModified || forceIncludeAllParams) {
      list = JSON.parse(JSON.stringify(this.list));
    }
    return list;
  }
}

/**
 * When onResponse extensions should be invoked: before or after processing the main response.
 */
export var ExtensionState = /*#__PURE__*/function (ExtensionState) {
  // Call onResponse before processing the response body. This is useful when your extension is
  // preparing the ground for the response body e.g. processing to-device messages before the
  // encrypted event arrives.
  ExtensionState["PreProcess"] = "ExtState.PreProcess";
  // Call onResponse after processing the response body. This is useful when your extension is
  // decorating data from the client, and you rely on MatrixClient.getRoom returning the Room object
  // e.g. room account data.
  ExtensionState["PostProcess"] = "ExtState.PostProcess";
  return ExtensionState;
}({});

/**
 * An interface that must be satisfied to register extensions
 */

/**
 * Events which can be fired by the SlidingSync class. These are designed to provide different levels
 * of information when processing sync responses.
 *  - RoomData: concerns rooms, useful for SlidingSyncSdk to update its knowledge of rooms.
 *  - Lifecycle: concerns callbacks at various well-defined points in the sync process.
 * Specifically, the order of event invocation is:
 *  - Lifecycle (state=RequestFinished)
 *  - RoomData (N times)
 *  - Lifecycle (state=Complete)
 */
export var SlidingSyncEvent = /*#__PURE__*/function (SlidingSyncEvent) {
  /**
   * This event fires when there are updates for a room. Fired as and when rooms are encountered
   * in the response.
   */
  SlidingSyncEvent["RoomData"] = "SlidingSync.RoomData";
  /**
   * This event fires at various points in the /sync loop lifecycle.
   *  - SlidingSyncState.RequestFinished: Fires after we receive a valid response but before the
   * response has been processed. Perform any pre-process steps here. If there was a problem syncing,
   * `err` will be set (e.g network errors).
   *  - SlidingSyncState.Complete: Fires after the response has been processed.
   */
  SlidingSyncEvent["Lifecycle"] = "SlidingSync.Lifecycle";
  return SlidingSyncEvent;
}({});

/**
 * Tuning for {@link SlidingSync.create} (and the auto-enable path in
 * `MatrixClient.startClient`). All fields are optional; the defaults give a
 * fast first paint (small windows, 1 event/room) with windows that grow to
 * cover every room.
 */

/**
 * Lean default required_state for the recency "all" list: just what an inbox
 * row + bundling + incoming-call detection need, so first paint stays fast.
 * Explicit because some servers (e.g. Continuwuity) do NOT honour the
 * `["*","*"]` wildcard — leaving rooms with no usable state. Callers can
 * override via {@link SlidingSyncCreateOpts.requiredState}.
 */
export var DEFAULT_SLIDING_SYNC_REQUIRED_STATE = [["m.room.create", ""], ["m.room.name", ""], ["m.room.avatar", ""], ["m.room.topic", ""], ["m.room.canonical_alias", ""], ["m.room.encryption", ""], ["m.room.member", MSC3575_STATE_KEY_ME], ["m.room.member", MSC3575_STATE_KEY_LAZY], ["io.element.functional_members", ""],
// bridge-bot list, so DM names exclude bots
["m.space.parent", MSC3575_WILDCARD], ["org.matrix.msc3401.call.member", MSC3575_WILDCARD], ["m.rtc.member", MSC3575_WILDCARD], ["eu.kiefte.issues.schema", ""], ["eu.kiefte.issue", MSC3575_WILDCARD]];

/**
 * Default required_state for the dedicated SPACES list: the lean inbox set plus
 * the space hierarchy edge (`m.space.child`) needed to build the tree — and
 * NOTHING heavier.
 *
 * This list grows its window to cover EVERY room the server reports for it, and
 * on a server that ignores the `room_types` filter (Continuwuity) that means
 * every room you're in. Carrying per-room `m.room.power_levels` / widgets /
 * emoji-pack state for all of them (as the subscription set below does) made the
 * spaces load slow and trickle in. A space only needs that heavy state when you
 * OPEN or manage it — at which point it gets a room subscription (the set below)
 * which carries it. So the list itself stays lean.
 */
export var DEFAULT_SLIDING_SYNC_SPACES_LIST_REQUIRED_STATE = [...DEFAULT_SLIDING_SYNC_REQUIRED_STATE, ["m.space.child", MSC3575_WILDCARD]];

/**
 * Default required_state for opened-room SUBSCRIPTIONS: the lean set PLUS the
 * heavier hierarchy/power/widget/emoji state an open room wants. (Previously
 * also used for the spaces list — see {@link DEFAULT_SLIDING_SYNC_SPACES_LIST_REQUIRED_STATE}
 * for why that was split out.)
 */
export var DEFAULT_SLIDING_SYNC_SPACES_REQUIRED_STATE = [...DEFAULT_SLIDING_SYNC_REQUIRED_STATE, ["m.space.child", MSC3575_WILDCARD], ["m.room.power_levels", ""], ["im.vector.modular.widgets", MSC3575_WILDCARD], ["im.ponies.room_emotes", MSC3575_WILDCARD]];

/**
 * SlidingSync is a high-level data structure which controls the majority of sliding sync.
 * It has no hooks into JS SDK except for needing a MatrixClient to perform the HTTP request.
 * This means this class (and everything it uses) can be used in isolation from JS SDK if needed.
 * To hook this up with the JS SDK, you need to use SlidingSyncSdk.
 */
export class SlidingSync extends TypedEventEmitter {
  /**
   * Create a new sliding sync instance
   * @param proxyBaseUrl - The base URL of the sliding sync proxy
   * @param lists - The lists to use for sliding sync.
   * @param roomSubscriptionInfo - The params to use for room subscriptions.
   * @param client - The client to use for /sync calls.
   * @param timeoutMS - The number of milliseconds to wait for a response.
   */
  constructor(proxyBaseUrl, lists, roomSubscriptionInfo, client, timeoutMS, connId) {
    super();
    this.proxyBaseUrl = proxyBaseUrl;
    this.roomSubscriptionInfo = roomSubscriptionInfo;
    this.client = client;
    this.timeoutMS = timeoutMS;
    this.connId = connId;
    _defineProperty(this, "lists", void 0);
    _defineProperty(this, "listModifiedCount", 0);
    _defineProperty(this, "terminated", false);
    // flag set when resend() is called because we cannot rely on detecting AbortError in JS SDK :(
    _defineProperty(this, "needsResend", false);
    // map of extension name to req/resp handler
    _defineProperty(this, "extensions", {});
    _defineProperty(this, "desiredRoomSubscriptions", new Set());
    // the *desired* room subscriptions
    _defineProperty(this, "confirmedRoomSubscriptions", new Set());
    // map of custom subscription name to the subscription
    _defineProperty(this, "customSubscriptions", new Map());
    // map of room ID to custom subscription name
    _defineProperty(this, "roomIdToCustomSubscription", new Map());
    _defineProperty(this, "pendingReq", void 0);
    _defineProperty(this, "abortController", void 0);
    this.lists = new Map();
    lists.forEach((list, key) => {
      this.lists.set(key, new SlidingList(list));
    });
  }

  /**
   * Convenience factory for the common "I want all my rooms" case, sized to
   * scale to very large accounts. Builds two lists — a dedicated `spaces` list
   * (spaces sort low by recency, so a plain window misses them) and a recency
   * `all` list for everything else whose window grows automatically until it
   * covers every room (small initial window = fast first paint). Opened rooms
   * should still be added via {@link SlidingSync#modifyRoomSubscriptions}.
   *
   * Pass the result straight to `client.startClient({ slidingSync })`.
   *
   * @param client - The client to sync with.
   * @param opts - Optional tuning (window size, growth step, required state,
   *   timeline limits, request timeout).
   */
  static create(client) {
    var _opts$requiredState, _opts$spacesRequiredS, _opts$subscriptionReq, _opts$windowSize, _opts$timelineLimit, _opts$roomSubscriptio, _opts$timeoutMS;
    var opts = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    // Per-list required_state lets callers keep the recency "all" list LEAN
    // (just what an inbox row needs) for a fast first paint, while the
    // spaces list still pulls the full hierarchy (m.space.child) and opened
    // rooms get everything via their subscription. The defaults are the
    // baked-in lean sets (NOT ["*","*"], which Continuwuity ignores); pass
    // requiredState to override.
    var requiredState = (_opts$requiredState = opts.requiredState) !== null && _opts$requiredState !== void 0 ? _opts$requiredState : DEFAULT_SLIDING_SYNC_REQUIRED_STATE;
    // The spaces LIST stays lean (it grows to cover every room; heavy
    // per-room state there is what made spaces trickle in slowly). Opened
    // rooms get the heavy set via their subscription.
    var spacesRequiredState = (_opts$spacesRequiredS = opts.spacesRequiredState) !== null && _opts$spacesRequiredS !== void 0 ? _opts$spacesRequiredS : DEFAULT_SLIDING_SYNC_SPACES_LIST_REQUIRED_STATE;
    var subscriptionRequiredState = (_opts$subscriptionReq = opts.subscriptionRequiredState) !== null && _opts$subscriptionReq !== void 0 ? _opts$subscriptionReq : DEFAULT_SLIDING_SYNC_SPACES_REQUIRED_STATE;
    var windowSize = (_opts$windowSize = opts.windowSize) !== null && _opts$windowSize !== void 0 ? _opts$windowSize : 100;
    var lists = new Map([["spaces", {
      ranges: [[0, 199]],
      timeline_limit: 0,
      required_state: spacesRequiredState,
      filters: {
        room_types: ["m.space"]
      }
    }], ["all", {
      ranges: [[0, windowSize - 1]],
      timeline_limit: (_opts$timelineLimit = opts.timelineLimit) !== null && _opts$timelineLimit !== void 0 ? _opts$timelineLimit : 1,
      required_state: requiredState,
      filters: {
        not_room_types: ["m.space"]
      }
    }]]);
    var roomSubscription = {
      timeline_limit: (_opts$roomSubscriptio = opts.roomSubscriptionTimelineLimit) !== null && _opts$roomSubscriptio !== void 0 ? _opts$roomSubscriptio : 50,
      required_state: subscriptionRequiredState
    };
    // 3s, not the classic 30s: Continuwuity holds the long-poll for the full
    // timeout instead of waking on a new event, so the timeout directly bounds
    // how long a just-received message / sent-echo takes to appear. Short poll
    // = snappy live updates (at the cost of more request volume) until the
    // server learns to wake on data. The initial request still uses timeout=0.
    var ss = new SlidingSync(client.baseUrl, lists, roomSubscription, client, (_opts$timeoutMS = opts.timeoutMS) !== null && _opts$timeoutMS !== void 0 ? _opts$timeoutMS : 3000);
    // Grow each list's window to cover EVERY room the server reports for that
    // list, so consumers that want "all rooms" / "all spaces" reliably get
    // them without managing ranges. The spaces list needs this too: on a
    // server that doesn't honour the room_types filter it degrades to a
    // recency list, so low-sorting space rooms sit beyond the initial window.
    //
    // We jump straight to full coverage (`[0, joinedCount]`) the first time a
    // sync reveals the count, rather than growing by `growBy` per sync. The
    // incremental approach needed N round-trips for N*growBy rooms — and on a
    // flaky link, where Complete events are rare, the window could stall and
    // never cover a large spaces list (a reported regression). The small
    // initial window still gives a fast first paint; this one extra growth
    // step then guarantees completeness. `growBy` is kept for API compat but
    // only caps how far a SINGLE step may jump (defaults large enough to
    // cover normal accounts in one go).
    var growStep = key => {
      var _ss$getListParams$ran, _ss$getListParams;
      var data = ss.getListData(key);
      if (!data) return;
      var end = (_ss$getListParams$ran = (_ss$getListParams = ss.getListParams(key)) === null || _ss$getListParams === void 0 || (_ss$getListParams = _ss$getListParams.ranges) === null || _ss$getListParams === void 0 || (_ss$getListParams = _ss$getListParams[0]) === null || _ss$getListParams === void 0 ? void 0 : _ss$getListParams[1]) !== null && _ss$getListParams$ran !== void 0 ? _ss$getListParams$ran : windowSize - 1;
      if (data.joinedCount > end + 1) {
        // Default: jump straight to full coverage so the list completes in
        // ONE growth step (robust on flaky links). If a caller passed an
        // explicit growBy, honour it as a per-step cap instead.
        var target = opts.growBy ? Math.min(end + opts.growBy, data.joinedCount) : data.joinedCount;
        ss.setListRanges(key, [[0, target]]);
      }
    };
    ss.on(SlidingSyncEvent.Lifecycle, (state, _resp, err) => {
      if (err || state !== SlidingSyncState.Complete) return;
      for (var key of ["all", "spaces"]) growStep(key);
    });
    return ss;
  }

  /**
   * Add a custom room subscription, referred to by an arbitrary name. If a subscription with this
   * name already exists, it is replaced. No requests are sent by calling this method.
   * @param name - The name of the subscription. Only used to reference this subscription in
   * useCustomSubscription.
   * @param sub - The subscription information.
   */
  addCustomSubscription(name, sub) {
    if (this.customSubscriptions.has(name)) {
      logger.warn("addCustomSubscription: ".concat(name, " already exists as a custom subscription, ignoring."));
      return;
    }
    this.customSubscriptions.set(name, sub);
  }

  /**
   * Use a custom subscription previously added via addCustomSubscription. No requests are sent
   * by calling this method. Use modifyRoomSubscriptions to resend subscription information.
   * @param roomId - The room to use the subscription in.
   * @param name - The name of the subscription. If this name is unknown, the default subscription
   * will be used.
   */
  useCustomSubscription(roomId, name) {
    // We already know about this custom subscription, as it is immutable,
    // we don't need to unconfirm the subscription.
    if (this.roomIdToCustomSubscription.get(roomId) === name) {
      return;
    }
    this.roomIdToCustomSubscription.set(roomId, name);
    // unconfirm this subscription so a resend() will send it up afresh.
    this.confirmedRoomSubscriptions.delete(roomId);
  }

  /**
   * Get the room index data for a list.
   * @param key - The list key
   * @returns The list data which contains the rooms in this list
   */
  getListData(key) {
    var data = this.lists.get(key);
    if (!data) {
      return null;
    }
    return {
      joinedCount: data.joinedCount
    };
  }

  /**
   * Get the full request list parameters for a list index. This function is provided for callers to use
   * in conjunction with setList to update fields on an existing list.
   * @param key - The list key to get the params for.
   * @returns A copy of the list params or undefined.
   */
  getListParams(key) {
    var params = this.lists.get(key);
    if (!params) {
      return null;
    }
    return params.getList(true);
  }

  /**
   * Set new ranges for an existing list. Calling this function when _only_ the ranges have changed
   * is more efficient than calling setList(index,list) as this function won't resend sticky params,
   * whereas setList always will.
   * @param key - The list key to modify
   * @param ranges - The new ranges to apply.
   * @returns A promise which resolves to the transaction ID when it has been received down sync
   * (or rejects with the transaction ID if the action was not applied e.g the request was cancelled
   * immediately after sending, in which case the action will be applied in the subsequent request)
   */
  setListRanges(key, ranges) {
    var list = this.lists.get(key);
    if (!list) {
      throw new Error("no list with key " + key);
    }
    list.updateListRange(ranges);
    this.resend();
  }

  /**
   * Add or replace a list. Calling this function will interrupt the /sync request to resend new
   * lists.
   * @param key - The key to modify
   * @param list - The new list parameters.
   * @returns A promise which resolves to the transaction ID when it has been received down sync
   * (or rejects with the transaction ID if the action was not applied e.g the request was cancelled
   * immediately after sending, in which case the action will be applied in the subsequent request)
   */
  setList(key, list) {
    var existingList = this.lists.get(key);
    if (existingList) {
      existingList.replaceList(list);
      this.lists.set(key, existingList);
    } else {
      this.lists.set(key, new SlidingList(list));
    }
    this.listModifiedCount += 1;
    this.resend();
  }

  /**
   * Get the room subscriptions for the sync API.
   * @returns A copy of the desired room subscriptions.
   */
  getRoomSubscriptions() {
    return new Set(Array.from(this.desiredRoomSubscriptions));
  }

  /**
   * Modify the room subscriptions for the sync API. Calling this function will interrupt the
   * /sync request to resend new subscriptions. If the /sync stream has not started, this will
   * prepare the room subscriptions for when start() is called.
   * @param s - The new desired room subscriptions.
   */
  modifyRoomSubscriptions(s) {
    this.desiredRoomSubscriptions = s;
    this.resend();
  }

  /**
   * Modify which events to retrieve for room subscriptions. Invalidates all room subscriptions
   * such that they will be sent up afresh.
   * @param rs - The new room subscription fields to fetch.
   */
  modifyRoomSubscriptionInfo(rs) {
    this.roomSubscriptionInfo = rs;
    this.confirmedRoomSubscriptions = new Set();
    this.resend();
  }

  /**
   * Register an extension to send with the /sync request.
   * @param ext - The extension to register.
   */
  registerExtension(ext) {
    if (this.extensions[ext.name()]) {
      throw new Error("registerExtension: ".concat(ext.name(), " already exists as an extension"));
    }
    this.extensions[ext.name()] = ext;
  }
  getExtensionRequest(isInitial) {
    var _this = this;
    return _asyncToGenerator(function* () {
      var ext = {};
      for (var extName in _this.extensions) {
        ext[extName] = yield _this.extensions[extName].onRequest(isInitial);
      }
      return ext;
    })();
  }
  onPreExtensionsResponse(ext) {
    var _this2 = this;
    return _asyncToGenerator(function* () {
      yield Promise.all(Object.keys(ext).map(/*#__PURE__*/function () {
        var _ref = _asyncToGenerator(function* (extName) {
          if (_this2.extensions[extName].when() == ExtensionState.PreProcess) {
            yield _this2.extensions[extName].onResponse(ext[extName]);
          }
        });
        return function (_x) {
          return _ref.apply(this, arguments);
        };
      }()));
    })();
  }
  onPostExtensionsResponse(ext) {
    var _this3 = this;
    return _asyncToGenerator(function* () {
      yield Promise.all(Object.keys(ext).map(/*#__PURE__*/function () {
        var _ref2 = _asyncToGenerator(function* (extName) {
          if (_this3.extensions[extName].when() == ExtensionState.PostProcess) {
            yield _this3.extensions[extName].onResponse(ext[extName]);
          }
        });
        return function (_x2) {
          return _ref2.apply(this, arguments);
        };
      }()));
    })();
  }

  /**
   * Invoke all attached room data listeners.
   * @param roomId - The room which received some data.
   * @param roomData - The raw sliding sync response JSON.
   */
  invokeRoomDataListeners(roomId, roomData) {
    var _this4 = this;
    return _asyncToGenerator(function* () {
      if (!roomData.required_state) {
        roomData.required_state = [];
      }
      if (!roomData.timeline) {
        roomData.timeline = [];
      }
      yield _this4.emitPromised(SlidingSyncEvent.RoomData, roomId, roomData);
    })();
  }

  /**
   * Invoke all attached lifecycle listeners.
   * @param state - The Lifecycle state
   * @param resp - The raw sync response JSON
   * @param err - Any error that occurred when making the request e.g. network errors.
   */
  invokeLifecycleListeners(state, resp, err) {
    this.emit(SlidingSyncEvent.Lifecycle, state, resp, err);
  }

  /**
   * Resend a Sliding Sync request. Used when something has changed in the request.
   */
  resend() {
    var _this$abortController;
    this.needsResend = true;
    (_this$abortController = this.abortController) === null || _this$abortController === void 0 || _this$abortController.abort();
    this.abortController = new AbortController();
  }

  /**
   * Stop syncing with the server.
   */
  stop() {
    var _this$abortController2;
    this.terminated = true;
    (_this$abortController2 = this.abortController) === null || _this$abortController2 === void 0 || _this$abortController2.abort();
    // remove all listeners so things can be GC'd
    this.removeAllListeners(SlidingSyncEvent.Lifecycle);
    this.removeAllListeners(SlidingSyncEvent.RoomData);
  }

  /**
   * localStorage key under which this account+device's last sliding-sync pos
   * is cached, so a reload can resume the connection rather than re-initialise.
   */
  posStorageKey() {
    var _this$client$getUserI, _this$client$getDevic, _this$connId;
    var userId = (_this$client$getUserI = this.client.getUserId()) !== null && _this$client$getUserI !== void 0 ? _this$client$getUserI : "@unknown:unknown";
    var deviceId = (_this$client$getDevic = this.client.getDeviceId()) !== null && _this$client$getDevic !== void 0 ? _this$client$getDevic : "nodevice";
    var conn = (_this$connId = this.connId) !== null && _this$connId !== void 0 ? _this$connId : "main";
    return "mxjssdk_sss_pos_".concat(userId, "_").concat(deviceId, "_").concat(conn);
  }
  restorePos() {
    try {
      var _localStorage$getItem;
      if (typeof localStorage === "undefined") return undefined;
      return (_localStorage$getItem = localStorage.getItem(this.posStorageKey())) !== null && _localStorage$getItem !== void 0 ? _localStorage$getItem : undefined;
    } catch (_unused) {
      return undefined; // private mode / blocked storage — start fresh
    }
  }
  persistPos(pos) {
    try {
      if (typeof localStorage === "undefined") return;
      var key = this.posStorageKey();
      if (pos === undefined) {
        localStorage.removeItem(key);
      } else {
        localStorage.setItem(key, pos);
      }
    } catch (_unused2) {
      // Non-fatal: we just fall back to a fresh pos next reload.
    }
  }

  /**
   * Re-setup this connection e.g in the event of an expired session.
   */
  resetup() {
    logger.warn("SlidingSync: resetting connection info");
    // resend sticky params and de-confirm all subscriptions
    this.lists.forEach(l => {
      l.setModified(true);
    });
    this.confirmedRoomSubscriptions = new Set(); // leave desired ones alone though!
    // reset the connection as we might be wedged
    this.resend();
  }

  /**
   * Start syncing with the server. Blocks until stopped.
   */
  start() {
    var _this5 = this;
    return _asyncToGenerator(function* () {
      _this5.abortController = new AbortController();

      // Restore the last pos so a page reload resumes the existing connection
      // instead of starting with pos=undefined — which forces the E2EE
      // extension to markAllTrackedUsersAsDirty (a full device-list re-query
      // and a UTD window) on every reload. If the server has expired the
      // connection the first request 400s and we reset to undefined below,
      // which correctly re-arms the dirty-marking.
      var currentPos = _this5.restorePos();
      var failures = 0;
      // After a response carries to-device events we're probably mid-handshake
      // (verification / key share), so poll fast for the next few rounds to
      // keep the multi-step exchange snappy, then relax back to the base
      // timeout when idle — responsiveness without an idle request storm.
      var boostPolls = 0;
      var BOOST_TIMEOUT_MS = 2000;
      var BOOST_ROUNDS = 4;
      var _loop = function* _loop() {
          _this5.needsResend = false;
          var resp;
          try {
            var _resp$lists, _resp$rooms, _resp$extensions;
            var reqLists = {};
            _this5.lists.forEach((l, key) => {
              reqLists[key] = l.getList(true);
            });
            // The very first request of a connection (no pos) must return the
            // initial window IMMEDIATELY rather than long-poll — the priority
            // rooms should paint before any timeout. timeout=0 means "send what
            // you have now". After that we long-poll (or fast-poll during a
            // to-device handshake). A 400/expiry resets pos, so a re-init is
            // fast too.
            var _isInitial = currentPos === undefined;
            var effectiveTimeout = _isInitial ? 0 : boostPolls > 0 ? Math.min(BOOST_TIMEOUT_MS, _this5.timeoutMS) : _this5.timeoutMS;
            if (!_isInitial && boostPolls > 0) boostPolls -= 1;
            var reqBody = _objectSpread({
              lists: reqLists,
              pos: currentPos,
              timeout: effectiveTimeout,
              clientTimeout: effectiveTimeout + BUFFER_PERIOD_MS,
              extensions: yield _this5.getExtensionRequest(_isInitial)
            }, _this5.connId ? {
              conn_id: _this5.connId
            } : {});
            // check if we are (un)subscribing to a room and modify request this one time for it
            var newSubscriptions = difference(_this5.desiredRoomSubscriptions, _this5.confirmedRoomSubscriptions);
            var unsubscriptions = difference(_this5.confirmedRoomSubscriptions, _this5.desiredRoomSubscriptions);
            if (unsubscriptions.size > 0) {
              reqBody.unsubscribe_rooms = Array.from(unsubscriptions);
            }
            if (newSubscriptions.size > 0) {
              reqBody.room_subscriptions = {};
              for (var roomId of newSubscriptions) {
                var customSubName = _this5.roomIdToCustomSubscription.get(roomId);
                var sub = _this5.roomSubscriptionInfo;
                if (customSubName && _this5.customSubscriptions.has(customSubName)) {
                  sub = _this5.customSubscriptions.get(customSubName);
                }
                reqBody.room_subscriptions[roomId] = sub;
              }
            }
            _this5.pendingReq = _this5.client.slidingSync(reqBody, _this5.proxyBaseUrl, _this5.abortController.signal);
            resp = yield _this5.pendingReq;
            currentPos = resp.pos;
            _this5.persistPos(currentPos);
            failures = 0; // a successful round-trip clears the backoff
            // update what we think we're subscribed to.
            for (var _roomId of newSubscriptions) {
              _this5.confirmedRoomSubscriptions.add(_roomId);
            }
            for (var _roomId2 of unsubscriptions) {
              _this5.confirmedRoomSubscriptions.delete(_roomId2);
            }
            // mark all these lists as having been sent as sticky so we don't keep sending sticky params
            _this5.lists.forEach(l => {
              l.setModified(false);
            });
            // set default empty values so we don't need to null check
            resp.lists = (_resp$lists = resp.lists) !== null && _resp$lists !== void 0 ? _resp$lists : {};
            resp.rooms = (_resp$rooms = resp.rooms) !== null && _resp$rooms !== void 0 ? _resp$rooms : {};
            resp.extensions = (_resp$extensions = resp.extensions) !== null && _resp$extensions !== void 0 ? _resp$extensions : {};
            // Mid to-device handshake? Poll fast for the next few rounds.
            var toDeviceResp = resp.extensions.to_device;
            if (Array.isArray(toDeviceResp === null || toDeviceResp === void 0 ? void 0 : toDeviceResp.events) && toDeviceResp.events.length > 0) {
              boostPolls = BOOST_ROUNDS;
            }
            Object.keys(resp.lists).forEach(key => {
              var list = _this5.lists.get(key);
              if (!list || !resp) {
                return;
              }
              list.joinedCount = resp.lists[key].count;
            });
            _this5.invokeLifecycleListeners(SlidingSyncState.RequestFinished, resp);
          } catch (err) {
            if (err.httpStatus) {
              _this5.invokeLifecycleListeners(SlidingSyncState.RequestFinished, null, err);
              if (err.httpStatus === 400) {
                // session probably expired TODO: assign an errcode
                // so drop state and re-request
                _this5.resetup();
                currentPos = undefined;
                _this5.persistPos(undefined); // drop the stale pos
                yield sleep(50); // in case the 400 was for something else; don't tightloop
                return 0; // continue
              } // else fallthrough to generic error handling
            } else if (_this5.needsResend || err.name === "AbortError") {
              return 0; // continue
              // don't sleep as we caused this error by abort()ing the request.
            }
            logger.error(err);
            // Exponential backoff with jitter instead of a flat 5s. A
            // transient drop (a suspended long-poll — ERR_NETWORK_IO_SUSPENDED
            // on tab background/sleep — or a brief network blip) recovers on
            // the first retry after ~1s rather than always stalling 5s; only
            // a persistently failing server backs off further. Capped at 30s.
            failures += 1;
            var backoffMs = Math.min(30000, 1000 * 2 ** Math.min(failures - 1, 5));
            var jitterMs = Math.floor(backoffMs * 0.2 * Math.random());
            yield sleep(backoffMs + jitterMs);
          }
          if (!resp) {
            return 0; // continue
          }
          yield _this5.onPreExtensionsResponse(resp.extensions);
          for (var _roomId3 in resp.rooms) {
            yield _this5.invokeRoomDataListeners(_roomId3, resp.rooms[_roomId3]);
          }
          _this5.invokeLifecycleListeners(SlidingSyncState.Complete, resp);
          yield _this5.onPostExtensionsResponse(resp.extensions);
        },
        _ret;
      while (!_this5.terminated) {
        _ret = yield* _loop();
        if (_ret === 0) continue;
      }
    })();
  }
}
var difference = (setA, setB) => {
  var diff = new Set(setA);
  for (var elem of setB) {
    diff.delete(elem);
  }
  return diff;
};
//# sourceMappingURL=sliding-sync.js.map