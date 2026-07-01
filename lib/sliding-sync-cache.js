import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
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
 * capped and the least-recently-active rooms are evicted. Anything goes wrong —
 * absent IndexedDB, corruption, quota — and we silently fall back to today's cold
 * start; the cache is a pure accelerator, never load-bearing.
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
/** Cap total cached rooms; the lowest-bump (least recently active) are evicted past this. */
var MAX_ROOMS = 512;
/** Coalesce a burst of room updates into one transaction. */
var FLUSH_DEBOUNCE_MS = 1500;
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
  if (next.limited) {
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
        var valid = records.filter(r => r && r.schema === SCHEMA_VERSION && r.data && r.roomId);
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
      this.records.set(roomId, {
        roomId,
        data: mergeRoomData(prev === null || prev === void 0 ? void 0 : prev.data, data),
        // receipt/accountData are computed fresh from the Room (already
        // merged truth) on every put, but keep the last-known copy when
        // a put omits them.
        receipt: receipt !== null && receipt !== void 0 ? receipt : prev === null || prev === void 0 ? void 0 : prev.receipt,
        accountData: accountData !== null && accountData !== void 0 ? accountData : prev === null || prev === void 0 ? void 0 : prev.accountData,
        bump: (_data$bump_stamp = data.bump_stamp) !== null && _data$bump_stamp !== void 0 ? _data$bump_stamp : Date.now(),
        ts: Date.now(),
        schema: SCHEMA_VERSION
      });
      this.dirty.add(roomId);
    } catch (_unused3) {
      return;
    }
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => void this.flush(), FLUSH_DEBOUNCE_MS);
    }
  }

  /** Write all dirty records, then evict down to MAX_ROOMS by lowest bump. */
  flush() {
    var _this2 = this;
    return _asyncToGenerator(function* () {
      if (_this2.flushTimer) {
        clearTimeout(_this2.flushTimer);
        _this2.flushTimer = null;
      }
      if (_this2.dirty.size === 0) return;
      var batch = [..._this2.dirty].map(roomId => _this2.records.get(roomId)).filter(r => !!r);
      _this2.dirty.clear();
      var db = yield _this2.open();
      if (!db) return;
      try {
        yield new Promise(resolve => {
          var t = _this2.tx(db, [STORE, META_STORE], "readwrite");
          var os = t.objectStore(STORE);
          for (var rec of batch) os.put(rec);
          t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
          t.onabort = () => resolve();
        });
        yield _this2.evict(db);
      } catch (e) {
        _this2.logger.warn("[sss-cache] flush failed", e);
      }
    })();
  }
  evict(db) {
    var _this3 = this;
    return _asyncToGenerator(function* () {
      try {
        var count = yield new Promise(resolve => {
          var r = _this3.tx(db, [STORE], "readonly").objectStore(STORE).count();
          r.onsuccess = () => resolve(r.result);
          r.onerror = () => resolve(0);
        });
        if (count <= MAX_ROOMS) return;
        var toDrop = count - MAX_ROOMS;
        yield new Promise(resolve => {
          var t = _this3.tx(db, [STORE], "readwrite");
          // Ascending by bump → drop the least recently active first.
          var cursorReq = t.objectStore(STORE).index("bump").openCursor(null, "next");
          var dropped = 0;
          cursorReq.onsuccess = () => {
            var cur = cursorReq.result;
            if (!cur || dropped >= toDrop) {
              resolve();
              return;
            }
            cur.delete();
            dropped++;
            cur.continue();
          };
          cursorReq.onerror = () => resolve();
          t.onabort = () => resolve();
        });
      } catch (e) {
        _this3.logger.warn("[sss-cache] evict failed", e);
      }
    })();
  }

  /** Drop the entire cache (used on schema change / reset). */
  clear() {
    var _this4 = this;
    return _asyncToGenerator(function* () {
      _this4.records.clear();
      _this4.dirty.clear();
      var db = yield _this4.open();
      if (!db) return;
      yield new Promise(resolve => {
        try {
          var t = _this4.tx(db, [STORE, META_STORE], "readwrite");
          t.objectStore(STORE).clear();
          t.objectStore(META_STORE).put(SCHEMA_VERSION, "schema");
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
          t.onabort = () => resolve();
        } catch (_unused4) {
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