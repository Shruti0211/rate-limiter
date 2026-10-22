// Copyright 2026 smahajkt
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * synchronized on the entire limiter serializes requests even when they belong to completely unrelated users and IPs.

Every accepted request performs map lookups and deque operations for each applicable policy, while rejected requests may still create empty buckets.

no bucket-level lock design
 */

package com.wba.returns.service.business.impl.tote.create;


import java.util.*;

public class SlidingWindowRateLimiter {

    public enum Scope {
        IP, USER, EXPERIENCE
    }

    private static final class WindowState {
        final Deque<Long> timestamps = new ArrayDeque<>();
    }


    private final Map<BucketKey, WindowState> buckets =
            new HashMap<>();

    // Global synchronization makes checking and recording atomic
    // for this single-process implementation.
    public synchronized Decision allow(
            List<Rule> rules, long nowMs) {

        long maxRetryMs = 0;
        Scope rejectedScope = null;

        // Phase 1: expire old requests and check every policy.
        for (Rule rule : rules) {
            BucketKey key = new BucketKey(rule.scope, rule.key);
            WindowState state = buckets.computeIfAbsent(
                    key, k -> new WindowState());

            long cutoff = nowMs - rule.windowMs;

            while (!state.timestamps.isEmpty()
                    && state.timestamps.peekFirst() <= cutoff) {
                state.timestamps.removeFirst();
            }

            if (state.timestamps.size() >= rule.limit) {
                long retryMs =
                        state.timestamps.peekFirst()
                                + rule.windowMs - nowMs;

                if (retryMs > maxRetryMs) {
                    maxRetryMs = retryMs;
                    rejectedScope = rule.scope;
                }
            }
        }

        // Phase 2: reject without consuming any other quota.
        if (rejectedScope != null) {
            return new Decision(false, maxRetryMs, rejectedScope);
        }

        // Phase 3: all policies passed; record the request.
        for (Rule rule : rules) {
            BucketKey key = new BucketKey(rule.scope, rule.key);
            buckets.get(key).timestamps.addLast(nowMs);
        }

        return new Decision(true, 0, null);
    }
}


/**
 * 
 * SlidingWindowRateLimiter limiter =
        new SlidingWindowRateLimiter();

List<SlidingWindowRateLimiter.Rule> rules = Arrays.asList(
        new SlidingWindowRateLimiter.Rule(
                SlidingWindowRateLimiter.Scope.IP,
                "203.0.113.10", 100, 60_000),

        new SlidingWindowRateLimiter.Rule(
                SlidingWindowRateLimiter.Scope.USER,
                "user-42", 60, 60_000),

        new SlidingWindowRateLimiter.Rule(
                SlidingWindowRateLimiter.Scope.EXPERIENCE,
                "checkout", 1_000, 60_000)
);

SlidingWindowRateLimiter.Decision result =
        limiter.allow(rules, System.currentTimeMillis());

if (result.allowed) {
        // Process request.
        } else {
        // Return HTTP 429 Too Many Requests.
        // Set Retry-After based on result.retryAfterMs.
        }
 * 
 * 
 * 
 */
