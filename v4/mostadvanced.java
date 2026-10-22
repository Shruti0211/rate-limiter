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
 * 
 * 
 * 
 Improvement : Memory : We do not create bucket for rejected requests. Suppose a user is making a request using a different IP, and user quota has expired, we reject the request and dont
 necessarily create a new bucket for this new IP
 Use counters instead of timestamps, so its not strict rather its an approximation


 * 
 * 
 */

private static final class Bucket {
    long windowId = Long.MIN_VALUE;
    int count;
}

private final ConcurrentHashMap<BucketKey, Bucket> buckets =
        new ConcurrentHashMap<>();

private static final int NUM_STRIPES = 1024;
private final ReentrantLock[] stripes = new ReentrantLock[NUM_STRIPES];

public CounterRateLimiter() {
    for (int i = 0; i < NUM_STRIPES; i++) {
        stripes[i] = new ReentrantLock();
    }
}

private int stripeIndex(BucketKey key) {
    return Math.floorMod(key.hashCode(), NUM_STRIPES);
}

public Decision allow(List<Rule> rules, LongSupplier clock) {
    if (rules == null || rules.isEmpty() || clock == null) {
        throw new IllegalArgumentException("Rules and clock required");
    }


   //The key identifies whose quota we're checking; the value contains that quota's configuration.
   // if there are same rules with same keys (Rule(IP, "10.0.0.1", 100, 60_000), Rule(IP, "10.0.0.1", 200, 60_000)) we throw exception
    Map<BucketKey, Rule> byKey = new HashMap<>();
    for (Rule rule : rules) {
        if (byKey.putIfAbsent(rule.key(), rule) != null) {
            throw new IllegalArgumentException("Duplicate bucket rule");
        }
    }

    List<BucketKey> keys = new ArrayList<>(byKey.keySet());
    Collections.sort(keys);

    // Acquire unique stripes in a consistent order.
    SortedSet<Integer> stripeIds = new TreeSet<>();
    for (BucketKey key : keys) {
        stripeIds.add(stripeIndex(key));
    }

    List<ReentrantLock> locked = new ArrayList<>();
    try {
        for (int id : stripeIds) {
            ReentrantLock lock = stripes[id];
            lock.lock();
            locked.add(lock);
        }

        long now = clock.getAsLong();
        long maxRetry = 0;
        Scope rejected = null;

        // Phase 1: Validate without creating or modifying buckets.
        for (BucketKey key : keys) {
            Rule rule = byKey.get(key);
            long windowId = Math.floorDiv(now, rule.windowMs);

            Bucket bucket = buckets.get(key);
            int effectiveCount =
                    bucket != null && bucket.windowId == windowId
                            ? bucket.count
                            : 0;

            if (effectiveCount >= rule.limit) {
                long retry = (windowId + 1) * rule.windowMs - now;

                if (retry > maxRetry) {
                    maxRetry = retry;
                    rejected = rule.scope;
                }
            }
        }

        // Rejected requests never create or update buckets.
        if (rejected != null) {
            return new Decision(false, maxRetry, rejected);
        }

        // Phase 2: Only allowed requests create/update buckets.
        for (BucketKey key : keys) {
            Rule rule = byKey.get(key);
            long windowId = Math.floorDiv(now, rule.windowMs);

            Bucket bucket = buckets.computeIfAbsent(
                    key, ignored -> new Bucket());

            if (bucket.windowId != windowId) {
                bucket.windowId = windowId;
                bucket.count = 0;
            }

            bucket.count++;
        }

        return new Decision(true, 0, null);

    } finally {
        for (int i = locked.size() - 1; i >= 0; i--) {
            locked.get(i).unlock();
        }
    }
}