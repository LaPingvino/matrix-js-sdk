import { type Focus } from "./focus.ts";
export interface LivekitFocusConfig extends Focus {
    type: "livekit";
    livekit_service_url: string;
}
export declare const isLivekitFocusConfig: (object: any) => object is LivekitFocusConfig;
export interface LivekitFocus extends LivekitFocusConfig {
    livekit_alias: string;
}
export declare const isLivekitFocus: (object: any) => object is LivekitFocus;
export interface LivekitFocusActive extends Focus {
    type: "livekit";
    /**
     * "multi_sfu" (Element Call's default since v0.21): every member publishes on its own
     * foci_preferred[0] and subscribes to the others'. "oldest_membership": everyone on the
     * oldest member's SFU (removed upstream in v44).
     */
    focus_selection: "oldest_membership" | "multi_sfu";
}
export declare const isLivekitFocusActive: (object: any) => object is LivekitFocusActive;
//# sourceMappingURL=LivekitFocus.d.ts.map