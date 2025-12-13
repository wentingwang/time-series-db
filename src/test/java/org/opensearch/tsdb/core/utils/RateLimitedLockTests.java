/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.MutableClock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimitedLockTests extends OpenSearchTestCase {

    public void testTryLockRespectsInterval() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // First acquisition should succeed
        assertTrue("First tryLock should succeed", lock.tryLock());
        lock.unlock();

        // Second acquisition immediately after should fail (interval not elapsed)
        assertFalse("Second tryLock should fail before interval elapses", lock.tryLock());
    }

    public void testTryLockSucceedsAfterInterval() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // First acquisition
        assertTrue("First tryLock should succeed", lock.tryLock());
        lock.unlock();

        // Advance time by less than interval
        clock.advance(50);
        assertFalse("tryLock should fail when only 50ms have elapsed", lock.tryLock());

        // Advance time to complete the interval
        clock.advance(50);
        assertTrue("tryLock should succeed after full 100ms interval (from original acquisition)", lock.tryLock());
        lock.unlock();
    }

    public void testLockDoesNotCheckInterval() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // First acquisition with lock()
        lock.lock();
        lock.unlock();

        // Second acquisition immediately after with lock() should succeed (doesn't check interval)
        lock.lock();
        lock.unlock();
    }

    public void testDynamicIntervalChange() {
        MutableClock clock = new MutableClock(1000);
        AtomicLong intervalMillis = new AtomicLong(100);
        RateLimitedLock lock = new RateLimitedLock(() -> TimeValue.timeValueMillis(intervalMillis.get()), clock);

        // First acquisition
        assertTrue("First tryLock should succeed", lock.tryLock());
        lock.unlock();

        // Change interval to shorter duration
        intervalMillis.set(30);

        // Advance time by new shorter interval
        clock.advance(30);
        assertTrue("tryLock should succeed after shorter interval", lock.tryLock());
        lock.unlock();
    }

    public void testZeroIntervalAlwaysSucceeds() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(0);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // Multiple rapid acquisitions should all succeed
        for (int i = 0; i < 10; i++) {
            assertTrue("tryLock should succeed with zero interval", lock.tryLock());
            lock.unlock();
        }
    }

    public void testLockUpdatesTimestamp() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // Use lock() to acquire
        lock.lock();
        lock.unlock();

        // tryLock should fail immediately after (interval not elapsed)
        assertFalse("tryLock should fail immediately after lock()", lock.tryLock());

        // Advance time and verify it succeeds
        clock.advance(100);
        assertTrue("tryLock should succeed after interval", lock.tryLock());
        lock.unlock();
    }

    public void testReentrantBehavior() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // Acquire lock multiple times in same thread (reentrant)
        lock.lock();
        lock.lock();
        assertTrue("Lock should be held", lock.isHeldByCurrentThread());
        assertEquals("Hold count should be 2", 2, lock.getHoldCount());

        lock.unlock();
        assertEquals("Hold count should be 1 after first unlock", 1, lock.getHoldCount());
        lock.unlock();
        assertEquals("Hold count should be 0 after second unlock", 0, lock.getHoldCount());
        assertFalse("Lock should not be held", lock.isHeldByCurrentThread());
    }

    public void testIntervalBoundaryCondition() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // First acquisition at time 1000
        assertTrue(lock.tryLock());
        lock.unlock();

        // At time 1099 (99ms later) should fail
        clock.set(1099);
        assertFalse("tryLock should fail at 99ms", lock.tryLock());

        // At time 1100 (100ms later) should succeed
        clock.set(1100);
        assertTrue("tryLock should succeed at exactly 100ms", lock.tryLock());
        lock.unlock();
    }

    public void testMixedLockAndTryLock() {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // Use tryLock first
        assertTrue(lock.tryLock());
        lock.unlock();

        // lock() should succeed immediately (doesn't check interval)
        lock.lock();
        lock.unlock();

        // But now tryLock should respect interval from the lock() call
        assertFalse("tryLock should fail after lock()", lock.tryLock());

        clock.advance(100);
        assertTrue("tryLock should succeed after interval", lock.tryLock());
        lock.unlock();
    }

    public void testTryLockFailsWhenLockHeldByAnotherThread() throws Exception {
        MutableClock clock = new MutableClock(1000);
        TimeValue interval = TimeValue.timeValueMillis(100);
        RateLimitedLock lock = new RateLimitedLock(() -> interval, clock);

        // first acquisition to set the lastAcquireTimeMillis
        assertTrue("First tryLock should succeed", lock.tryLock());
        lock.unlock();

        // advance time past the interval so the interval check passes
        clock.advance(200);

        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch canRelease = new CountDownLatch(1);

        // acquire lock in another thread and hold it
        Thread otherThread = new Thread(() -> {
            lock.lock();
            try {
                lockAcquired.countDown();
                canRelease.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        });
        otherThread.start();

        lockAcquired.await();

        // ensure interval check passes (500ms > 300ms), so failure is due to another thread holding the lock
        clock.advance(200);
        assertFalse("tryLock should fail when lock is held by another thread despite interval passing", lock.tryLock());

        canRelease.countDown();
        otherThread.join();

        // other thread released the lock, tryLock should succeed with zero time advancement since the last attempt
        assertTrue("tryLock should succeed after other thread releases lock", lock.tryLock());
        lock.unlock();
    }
}
