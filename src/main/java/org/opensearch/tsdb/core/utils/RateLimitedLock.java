/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.opensearch.common.unit.TimeValue;

import java.time.Clock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * A rate-limited lock that prevents acquiring the lock more frequently than a configured interval.
 *
 * This lock extends ReentrantLock and adds rate limiting based on time intervals.
 * If tryLock() is called before the configured interval has elapsed since the last successful acquisition,
 * it will return false without attempting to acquire the lock.
 */
public class RateLimitedLock extends ReentrantLock {
    private volatile long lastAcquireTimeMillis = 0;
    private final Supplier<TimeValue> intervalSupplier;
    private final Clock clock;

    /**
     * Constructs a RateLimitedLock with a dynamic interval supplier and custom clock. Package-private for testing.
     *
     * @param intervalSupplier supplier that provides the minimum interval between lock acquisitions
     * @param clock clock to use for time measurements
     */
    public RateLimitedLock(Supplier<TimeValue> intervalSupplier, Clock clock) {
        this.intervalSupplier = intervalSupplier;
        this.clock = clock;
    }

    /**
     * Attempts to acquire the lock without waiting, respecting the configured time interval.
     *
     * @return true if the lock was acquired and the interval has elapsed, false otherwise
     */
    @Override
    public boolean tryLock() {
        long now = clock.millis();
        long interval = intervalSupplier.get().millis();

        // Check if minimum interval has elapsed since last acquisition
        if (Math.max(0, now - lastAcquireTimeMillis) < interval) {
            return false;
        }

        boolean acquired = super.tryLock();
        if (acquired) {
            lastAcquireTimeMillis = now;
        }
        return acquired;
    }

    /**
     * Acquires the lock, waiting if necessary. This does not respect the configured time interval, but locks as soon as possible.
     */
    @Override
    public void lock() {
        super.lock();
        lastAcquireTimeMillis = clock.millis();
    }
}
