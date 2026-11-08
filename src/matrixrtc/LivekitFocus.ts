/*
Copyright 2023 New Vector Ltd

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

import { type Focus } from "./focus.ts";

export interface LivekitFocusConfig extends Focus {
    type: "livekit";
    livekit_service_url: string;
}

export const isLivekitFocusConfig = (object: any): object is LivekitFocusConfig =>
    object.type === "livekit" && "livekit_service_url" in object;

export interface LivekitFocus extends LivekitFocusConfig {
    livekit_alias: string;
}

export const isLivekitFocus = (object: any): object is LivekitFocus =>
    isLivekitFocusConfig(object) && "livekit_alias" in object;

export interface LivekitFocusActive extends Focus {
    type: "livekit";
    /**
     * "multi_sfu" (Element Call's default since v0.21): every member publishes on its own
     * foci_preferred[0] and subscribes to the others'. "oldest_membership": everyone on the
     * oldest member's SFU (removed upstream in v44).
     */
    focus_selection: "oldest_membership" | "multi_sfu";
}
export const isLivekitFocusActive = (object: any): object is LivekitFocusActive =>
    object.type === "livekit" && "focus_selection" in object;
