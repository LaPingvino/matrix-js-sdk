import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
var _excluded = ["prev_batch"];
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
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

var DB_VERSION = 1;
/** Bump when the stored record shape changes incompatibly; mismatch wipes the store. */
var SCHEMA_VERSION = 2;
var STORE = "rooms";
var META_STORE = "meta";
/** Cap timeline events per room. Subscriptions deliver at most 50; this is a safety ceiling. */
var MAX_TIMELINE = 100;
/**
 * Cap rooms cached WITH a timeline. Past this, the least recently active are
 * SHELLED (timeline dropped, state kept) — not deleted. See {@link prune}.
 */
var MAX_ROOMS = 512;
/**
 * Hard ceiling on total records, shells included. A shell is state-only and
 * small, so this sits far above any real account; past it we genuinely delete,
 * which is the one path that can desynchronise us from a resumed pos (see
 * {@link droppedRecords}).
 */
var MAX_RECORDS = 8192;
/** Coalesce a burst of room updates into one transaction. */
var FLUSH_DEBOUNCE_MS = 1500;
/** Does this room's cached state say it is a space? */
export function isSpaceData(data) {
  var _data$required_state;
  return ((_data$required_state = data.required_state) !== null && _data$required_state !== void 0 ? _data$required_state : []).some(e => {
    var _e$content;
    return e.type === "m.room.create" && e.state_key === "" && ((_e$content = e.content) === null || _e$content === void 0 ? void 0 : _e$content.type) === "m.space";
  });
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
export function shellRecord(rec) {
  var _rec$data = rec.data,
    {
      prev_batch: _dropped
    } = _rec$data,
    rest = _objectWithoutProperties(_rec$data, _excluded);
  return _objectSpread(_objectSpread({}, rec), {}, {
    data: _objectSpread(_objectSpread({}, rest), {}, {
      timeline: [],
      limited: true
    })
  });
}

/**
 * Repair a record that would rehydrate into a room nothing can fill.
 *
 * Events with no `prev_batch` is that shape: the timeline paints, but
 * Room.backgroundBackfill bails on a missing backwards token, so the room sits at
 * whatever few events the record held until someone scrolls by hand. Records like
 * this exist in the wild — a shelled room merged a non-limited delta before
 * mergeRoomData learned to take the delta's token — so drop the timeline and let
 * the live sync re-deliver a window WITH a token. Costs one room's preview on one
 * boot; the alternative is a chat stuck on its last few messages.
 */
export var repairUnfillable = rec => {
  var _rec$data$timeline$le, _rec$data$timeline;
  return ((_rec$data$timeline$le = (_rec$data$timeline = rec.data.timeline) === null || _rec$data$timeline === void 0 ? void 0 : _rec$data$timeline.length) !== null && _rec$data$timeline$le !== void 0 ? _rec$data$timeline$le : 0) > 0 && !rec.data.prev_batch ? _objectSpread(_objectSpread({}, rec), {}, {
    data: _objectSpread(_objectSpread({}, rec.data), {}, {
      timeline: [],
      limited: true
    })
  }) : rec;
};
var isShell = rec => {
  var _rec$data$timeline$le2, _rec$data$timeline2;
  return ((_rec$data$timeline$le2 = (_rec$data$timeline2 = rec.data.timeline) === null || _rec$data$timeline2 === void 0 ? void 0 : _rec$data$timeline2.length) !== null && _rec$data$timeline$le2 !== void 0 ? _rec$data$timeline$le2 : 0) === 0;
};

/**
 * Decide what to shell and what to delete. Pure; exported for tests.
 * See {@link SlidingSyncCache.prune} for the reasoning behind the two caps.
 */
export function planPrune(records) {
  var evictable = records.filter(r => !r.pin);
  var byBumpAscending = (a, b) => a.bump - b.bump;
  var full = evictable.filter(r => !isShell(r)).sort(byBumpAscending);
  var shell = full.slice(0, Math.max(0, full.length - MAX_ROOMS)).map(r => r.roomId);

  // Shelling frees no RECORDS, so the total cap is measured against the whole
  // set and satisfied only by deleting. Records shelled above are already
  // counted here as the shells they are about to become.
  var shellSet = new Set(shell);
  var drop = records.length > MAX_RECORDS ? evictable.filter(r => isShell(r) || shellSet.has(r.roomId)).sort(byBumpAscending).slice(0, records.length - MAX_RECORDS).map(r => r.roomId) : [];
  var dropSet = new Set(drop);
  return {
    shell: shell.filter(id => !dropSet.has(id)),
    drop
  };
}
function cacheDisabled() {
  try {
    var _globalThis$localStor;
    return ((_globalThis$localStor = globalThis.localStorage) === null || _globalThis$localStor === void 0 ? void 0 : _globalThis$localStor.getItem("mxjssdk_sss_cache_disable")) === "1";
  } catch (_unused) {
    return false;
  }
}

/** Strip a room's data down to the bounded slice we persist. */
function curate(data) {
  var _data$timeline;
  var timeline = (_data$timeline = data.timeline) !== null && _data$timeline !== void 0 ? _data$timeline : [];
  return _objectSpread(_objectSpread({}, data), {}, {
    // Keep the most RECENT events (the tail); back-pagination uses prev_batch.
    timeline: timeline.length > MAX_TIMELINE ? timeline.slice(timeline.length - MAX_TIMELINE) : timeline
  });
}
var eventIdOf = e => e.event_id;

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
export function mergeRoomData(prev, next) {
  var _prev$timeline, _next$timeline;
  if (!prev || next.initial) {
    return curate(next);
  }
  // Keep-last-known scalars: only let DEFINED incoming fields overwrite.
  var overlay = {};
  for (var [k, v] of Object.entries(next)) {
    if (v !== undefined && v !== null) overlay[k] = v;
  }
  var merged = _objectSpread(_objectSpread({}, prev), overlay);

  // required_state: union by (type, state_key), incoming wins.
  var state = new Map();
  for (var ev of (_prev$required_state = prev.required_state) !== null && _prev$required_state !== void 0 ? _prev$required_state : []) {
    var _prev$required_state;
    state.set("".concat(ev.type, "|").concat(ev.state_key), ev);
  }
  for (var _ev of (_next$required_state = next.required_state) !== null && _next$required_state !== void 0 ? _next$required_state : []) {
    var _next$required_state;
    state.set("".concat(_ev.type, "|").concat(_ev.state_key), _ev);
  }
  merged.required_state = [...state.values()];
  var prevTimeline = (_prev$timeline = prev.timeline) !== null && _prev$timeline !== void 0 ? _prev$timeline : [];
  var nextTimeline = (_next$timeline = next.timeline) !== null && _next$timeline !== void 0 ? _next$timeline : [];
  if (prevTimeline.length === 0) {
    // Nothing to be contiguous WITH. A shelled room (see shellRecord) is
    // exactly this: state kept, timeline and prev_batch dropped. Appending to
    // an empty tail and then keeping the cached prev_batch — which is
    // undefined — would cache a few events with NO pagination token, and the
    // next boot would replay a room showing only its last few messages with
    // no way to scroll back. With no prior events the delta's window IS the
    // whole timeline, so its token is by definition the right start token.
    merged.timeline = nextTimeline;
    merged.prev_batch = next.prev_batch;
  } else if (next.limited) {
    // Doesn't connect to our cached tail: replace. prev_batch (if any)
    // matches the new window's start; an absent one wipes the old token,
    // which no longer describes this timeline's start either.
    merged.timeline = nextTimeline;
    merged.prev_batch = next.prev_batch;
  } else {
    var seen = new Set(prevTimeline.map(eventIdOf));
    var appended = [...prevTimeline, ...nextTimeline.filter(e => !eventIdOf(e) || !seen.has(eventIdOf(e)))];
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
  constructor(userId, logger) {
    this.logger = logger;
    _defineProperty(this, "dbPromise", null);
    /**
     * Session store-of-record: the MERGED cached record per room, seeded from
     * IndexedDB by {@link loadAll} and updated by every {@link put}. Deltas
     * merge against this synchronously (no IDB read on the put path); flush
     * writes the dirty subset out. Memory is bounded by the account's room
     * count, same order as the SDK's own Room store.
     */
    _defineProperty(this, "records", new Map());
    _defineProperty(this, "dirty", new Set());
    /** Records to delete from IDB on the next flush (see {@link prune}). */
    _defineProperty(this, "deletes", new Set());
    /** Set when a record was genuinely deleted — this session or a previous one. */
    _defineProperty(this, "dropped", false);
    _defineProperty(this, "flushTimer", null);
    _defineProperty(this, "closed", false);
    _defineProperty(this, "dbName", void 0);
    this.dbName = "mxjssdk_sss_cache_".concat(userId !== null && userId !== void 0 ? userId : "anon");
  }
  get idb() {
    try {
      return globalThis.indexedDB;
    } catch (_unused2) {
      return undefined;
    }
  }
  open() {
    if (this.dbPromise) return this.dbPromise;
    this.dbPromise = new Promise(resolve => {
      var idb = this.idb;
      if (!idb || cacheDisabled()) {
        resolve(null);
        return;
      }
      var req;
      try {
        req = idb.open(this.dbName, DB_VERSION);
      } catch (e) {
        this.logger.warn("[sss-cache] open threw; disabling cache", e);
        resolve(null);
        return;
      }
      req.onupgradeneeded = () => {
        var db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          var os = db.createObjectStore(STORE, {
            keyPath: "roomId"
          });
          os.createIndex("bump", "bump");
        }
        if (!db.objectStoreNames.contains(META_STORE)) {
          db.createObjectStore(META_STORE);
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => {
        this.logger.warn("[sss-cache] open failed; disabling cache", req.error);
        resolve(null);
      };
      req.onblocked = () => resolve(null);
    });
    return this.dbPromise;
  }
  tx(db, stores, mode) {
    return db.transaction(stores, mode);
  }

  /**
   * Load all cached rooms for replay, newest-active first, after validating the
   * schema version (a mismatch wipes the store and returns nothing). Returns the
   * curated {@link MSC3575RoomData} with `initial`/`limited` forced so the caller
   * can feed it straight through the live ingestion path.
   */
  loadAll() {
    var _this = this;
    return _asyncToGenerator(function* () {
      var db = yield _this.open();
      if (!db) return [];
      try {
        var okSchema = yield new Promise(resolve => {
          var r = _this.tx(db, [META_STORE], "readonly").objectStore(META_STORE).get("schema");
          r.onsuccess = () => {
            var _r$result;
            return resolve(((_r$result = r.result) !== null && _r$result !== void 0 ? _r$result : SCHEMA_VERSION) === SCHEMA_VERSION);
          };
          r.onerror = () => resolve(false);
        });
        if (!okSchema) {
          _this.logger.info("[sss-cache] schema changed; clearing cache");
          yield _this.clear();
          return [];
        }
        // Did a previous session genuinely DELETE any record? If so the
        // persisted pos is no longer safe to resume — see droppedRecords.
        _this.dropped = yield new Promise(resolve => {
          var r = _this.tx(db, [META_STORE], "readonly").objectStore(META_STORE).get("dropped");
          r.onsuccess = () => resolve(r.result === true);
          r.onerror = () => resolve(false);
        });
        var records = yield new Promise(resolve => {
          var out = [];
          var idx = _this.tx(db, [STORE], "readonly").objectStore(STORE).index("bump");
          // Descending by bump → most recently active rooms replay first.
          var cursorReq = idx.openCursor(null, "prev");
          cursorReq.onsuccess = () => {
            var cur = cursorReq.result;
            if (!cur) {
              resolve(out);
              return;
            }
            out.push(cur.value);
            cur.continue();
          };
          cursorReq.onerror = () => resolve(out);
        });
        var valid = records.filter(r => r && r.schema === SCHEMA_VERSION && r.data && r.roomId)
        // See repairUnfillable: events with no token rehydrate into a
        // room that nothing can fill automatically.
        .map(repairUnfillable);
        // Pinned rooms (spaces) replay FIRST, then the rest by recency. The
        // sidebar's structure is what the whole UI hangs off, so paint it
        // before the chatter rather than somewhere in the middle of 500
        // rooms. Stable within each group (the cursor already ordered by
        // bump descending).
        valid.sort((a, b) => {
          var _b$pin, _a$pin;
          return ((_b$pin = b.pin) !== null && _b$pin !== void 0 ? _b$pin : 0) - ((_a$pin = a.pin) !== null && _a$pin !== void 0 ? _a$pin : 0);
        });
        // Seed the merge base: subsequent live DELTAS for these rooms merge
        // against what we just loaded rather than replacing it.
        for (var r of valid) {
          if (!_this.records.has(r.roomId)) _this.records.set(r.roomId, r);
        }
        return valid.map(r => ({
          roomId: r.roomId,
          data: _objectSpread(_objectSpread({}, r.data), {}, {
            initial: true,
            limited: true
          }),
          receipt: r.receipt,
          accountData: r.accountData
        }));
      } catch (e) {
        _this.logger.warn("[sss-cache] loadAll failed", e);
        return [];
      }
    })();
  }

  /**
   * Merge a room's data into the session record (deltas merge, initial
   * replaces — see {@link mergeRoomData}) and queue it for persistence
   * (debounced + coalesced).
   */
  put(roomId, data, receipt, accountData) {
    if (this.closed || !this.idb || cacheDisabled()) return;
    try {
      var _data$bump_stamp;
      var prev = this.records.get(roomId);
      var merged = mergeRoomData(prev === null || prev === void 0 ? void 0 : prev.data, data);
      this.records.set(roomId, _objectSpread(_objectSpread({
        roomId,
        data: merged,
        // receipt/accountData are computed fresh from the Room (already
        // merged truth) on every put, but keep the last-known copy when
        // a put omits them.
        receipt: receipt !== null && receipt !== void 0 ? receipt : prev === null || prev === void 0 ? void 0 : prev.receipt,
        accountData: accountData !== null && accountData !== void 0 ? accountData : prev === null || prev === void 0 ? void 0 : prev.accountData,
        bump: (_data$bump_stamp = data.bump_stamp) !== null && _data$bump_stamp !== void 0 ? _data$bump_stamp : Date.now()
      }, isSpaceData(merged) ? {
        pin: 1
      } : {}), {}, {
        ts: Date.now(),
        schema: SCHEMA_VERSION
      }));
      this.dirty.add(roomId);
    } catch (_unused3) {
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
  get droppedRecords() {
    return this.dropped;
  }

  /** Acknowledge {@link droppedRecords}: the caller has dropped the pos. */
  clearDropped() {
    var _this2 = this;
    return _asyncToGenerator(function* () {
      _this2.dropped = false;
      var db = yield _this2.open();
      if (!db) return;
      yield new Promise(resolve => {
        try {
          var t = _this2.tx(db, [META_STORE], "readwrite");
          t.objectStore(META_STORE).delete("dropped");
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
          t.onabort = () => resolve();
        } catch (_unused4) {
          resolve();
        }
      });
    })();
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
  prune() {
    var {
      shell,
      drop
    } = planPrune([...this.records.values()]);
    for (var roomId of shell) {
      var rec = this.records.get(roomId);
      if (!rec) continue;
      this.records.set(roomId, shellRecord(rec));
      this.dirty.add(roomId);
    }
    for (var _roomId of drop) {
      this.records.delete(_roomId);
      this.dirty.delete(_roomId);
      this.deletes.add(_roomId);
      this.dropped = true;
    }
  }

  /** Prune to the caps, then write everything pending in one transaction. */
  flush() {
    var _this3 = this;
    return _asyncToGenerator(function* () {
      if (_this3.flushTimer) {
        clearTimeout(_this3.flushTimer);
        _this3.flushTimer = null;
      }
      if (_this3.dirty.size === 0 && _this3.deletes.size === 0) return;
      _this3.prune();
      var batch = [..._this3.dirty].map(roomId => _this3.records.get(roomId)).filter(r => !!r);
      var deletes = [..._this3.deletes];
      var dropped = _this3.dropped;
      _this3.dirty.clear();
      _this3.deletes.clear();
      var db = yield _this3.open();
      if (!db) return;
      try {
        yield new Promise(resolve => {
          var t = _this3.tx(db, [STORE, META_STORE], "readwrite");
          var os = t.objectStore(STORE);
          for (var rec of batch) os.put(rec);
          for (var roomId of deletes) os.delete(roomId);
          t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
          // Same transaction as the deletes: the flag can never be lost
          // while the deletion it describes survives.
          if (dropped) t.objectStore(META_STORE).put(true, "dropped");
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
          t.onabort = () => resolve();
        });
      } catch (e) {
        _this3.logger.warn("[sss-cache] flush failed", e);
      }
    })();
  }

  /** Drop the entire cache (used on schema change / reset). */
  clear() {
    var _this4 = this;
    return _asyncToGenerator(function* () {
      _this4.records.clear();
      _this4.dirty.clear();
      _this4.deletes.clear();
      // A cleared cache restores nothing, so the caller drops the pos on the
      // `replayed === 0` path anyway; leaving the flag set would force a
      // second, pointless resync on the boot after that.
      _this4.dropped = false;
      var db = yield _this4.open();
      if (!db) return;
      yield new Promise(resolve => {
        try {
          var t = _this4.tx(db, [STORE, META_STORE], "readwrite");
          t.objectStore(STORE).clear();
          t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
          t.objectStore(META_STORE).delete("dropped");
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
          t.onabort = () => resolve();
        } catch (_unused5) {
          resolve();
        }
      });
    })();
  }

  /** Flush any pending writes and stop accepting new ones. */
  stop() {
    var _this5 = this;
    return _asyncToGenerator(function* () {
      _this5.closed = true;
      yield _this5.flush();
    })();
  }
}
//# sourceMappingURL=sliding-sync-cache.js.map