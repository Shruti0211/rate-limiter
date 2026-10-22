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


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

public final class RateLimiter {

    public enum Scope {
        IP, USER, EXPERIENCE
    }

    public static final class Rule {
        final Scope scope;
        final String identity;
        final int limit;
        final long windowMs;

        public Rule(Scope scope, String identity,
                    int limit, long windowMs) {
            if (scope == null || identity == null
                    || identity.isEmpty()
                    || limit <= 0 || windowMs <= 0) {
                throw new IllegalArgumentException("Invalid rule");
            }

            this.scope = scope;
            this.identity = identity;
            this.limit = limit;
            this.windowMs = windowMs;
        }

        BucketKey key() {
            return new BucketKey(scope, identity);
        }
    }

    private static final class BucketKey
            implements Comparable<BucketKey> {
        final Scope scope;
        final String identity;

        BucketKey(Scope scope, String identity) {
            this.scope = scope;
            this.identity = identity;
        }

        @Override
        public int compareTo(BucketKey other) {
            int c = scope.compareTo(other.scope);
            return c != 0 ? c : identity.compareTo(other.identity);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BucketKey)) return false;
            BucketKey other = (BucketKey) obj;
            return scope == other.scope
                    && identity.equals(other.identity);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, identity);
        }
    }

    private static final class Bucket {
        final ReentrantLock lock = new ReentrantLock();
        final Deque<Long> timestamps = new ArrayDeque<>();
    }

    public static final class Decision {
        public final boolean allowed;
        public final long retryAfterMs;
        public final Scope rejectedScope;

        Decision(boolean allowed, long retryAfterMs,
                 Scope rejectedScope) {
            this.allowed = allowed;
            this.retryAfterMs = retryAfterMs;
            this.rejectedScope = rejectedScope;
        }
    }

    private final ConcurrentHashMap<BucketKey, Bucket> buckets =
            new ConcurrentHashMap<>();

    public Decision allow(List<Rule> rules, LongSupplier clock) {
        if (rules == null || clock == null || rules.isEmpty()) {
            throw new IllegalArgumentException(
                    "Rules and clock are required");
        }

        // Resolve keys and lock in a consistent global order.
        // A request should have at most one rule per bucket.
        Map<BucketKey, Rule> uniqueRules = new HashMap<>();
        for (Rule rule : rules) {
            Rule previous = uniqueRules.putIfAbsent(rule.key(), rule);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate rule for " + rule.scope
                                + ": " + rule.identity);
            }
        }

        List<BucketKey> keys = new ArrayList<>(uniqueRules.keySet());
        Collections.sort(keys);

        List<Bucket> locked = new ArrayList<>(keys.size());

        try {
            // Create and lock every bucket in deterministic order.
            for (BucketKey key : keys) {
                Bucket bucket = buckets.computeIfAbsent(
                        key, ignored -> new Bucket());
                bucket.lock.lock();
                locked.add(bucket);
            }

            // Sample time only after all relevant locks are acquired.
            long now = clock.getAsLong();
            long maxRetryMs = 0;
            Scope rejectedScope = null;

            // Phase 1: prune expired timestamps and check all limits.
            for (BucketKey key : keys) {
                Rule rule = uniqueRules.get(key);
                Bucket bucket = buckets.get(key);
                long cutoff = now - rule.windowMs;

                while (!bucket.timestamps.isEmpty()
                        && bucket.timestamps.peekFirst() <= cutoff) {
                    bucket.timestamps.removeFirst();
                }

                if (bucket.timestamps.size() >= rule.limit) {
                    long retryMs = Math.max(
                            0,
                            bucket.timestamps.peekFirst()
                                    + rule.windowMs - now);

                    if (retryMs > maxRetryMs) {
                        maxRetryMs = retryMs;
                        rejectedScope = rule.scope;
                    }
                }
            }

            // Phase 2: reject without recording in any bucket.
            if (rejectedScope != null) {
                return new Decision(false, maxRetryMs, rejectedScope);
            }

            // Phase 3: record only after every rule passes.
            for (Bucket bucket : locked) {
                bucket.timestamps.addLast(now);
            }

            return new Decision(true, 0, null);

        } finally {
            // Reverse order is conventional and keeps lock discipline clear.
            for (int i = locked.size() - 1; i >= 0; i--) {
                locked.get(i).lock.unlock();
            }
        }
    }
}
