/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable clock implementation for testing that allows advancing time.
 */
public class MutableClock extends Clock {
    private final AtomicLong currentMillis;
    private final ZoneId zone;

    public MutableClock(long initialMillis) {
        this.currentMillis = new AtomicLong(initialMillis);
        this.zone = ZoneId.of("UTC");
    }

    public void advance(long millis) {
        currentMillis.addAndGet(millis);
    }

    public void set(long millis) {
        currentMillis.set(millis);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(currentMillis.get());
    }

    @Override
    public long millis() {
        return currentMillis.get();
    }
}
