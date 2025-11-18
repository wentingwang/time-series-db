/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.test.OpenSearchTestCase;

public class XORChunkTests extends OpenSearchTestCase {

    public void testXorRead() throws Exception {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        for (long i = 0; i < 120_000; i += 1000) {
            double value = i + (double) i / 10 + (double) i / 100 + (double) i / 1000;
            appender.append(i, value);
        }

        // Test chunk properties
        assertEquals("Encoding should be XOR", Encoding.XOR, chunk.encoding());
        assertEquals("Should have 120 samples", 120, chunk.numSamples());
        assertTrue("Chunk should have compressed data", chunk.bytesSize() > 0);
        // Test iterator functionality
        ChunkIterator iterator = chunk.iterator();
        int count = 0;
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            long expectedTimestamp = count * 1000L;
            double expectedValue = expectedTimestamp + (double) expectedTimestamp / 10 + (double) expectedTimestamp / 100
                + (double) expectedTimestamp / 1000;

            assertEquals("Timestamp mismatch at index " + count, expectedTimestamp, tv.timestamp());
            assertEquals("Value mismatch at index " + count, expectedValue, tv.value(), 1e-10);

            count++;
        }

        assertEquals("Should have read all 120 samples", 120, count);
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testAppenderStateRestoration() throws Exception {
        // Test that appender() correctly restores state from existing chunk data
        XORChunk chunk = new XORChunk();
        ChunkAppender appender1 = chunk.appender();

        // Add initial samples
        appender1.append(1000, 10.0);
        appender1.append(2000, 20.0);
        appender1.append(3000, 30.0);

        // Get a new appender - should restore state from existing data
        ChunkAppender appender2 = chunk.appender();

        // Add more samples with the new appender
        appender2.append(4000, 40.0);
        appender2.append(5000, 50.0);

        // Verify all samples are readable
        ChunkIterator iterator = chunk.iterator();
        double[] expectedValues = { 10.0, 20.0, 30.0, 40.0, 50.0 };
        long[] expectedTimestamps = { 1000, 2000, 3000, 4000, 5000 };

        int count = 0;
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();

            assertTrue("Too many samples read: " + count, count < expectedValues.length);

            assertEquals("Sample " + count + " timestamp mismatch", expectedTimestamps[count], tv.timestamp());

            assertEquals("Sample " + count + " value mismatch", expectedValues[count], tv.value(), 0.001);

            count++;
        }

        assertNull("Iterator should not have errors", iterator.error());
        assertEquals("Expected 5 samples", 5, count);
    }

    public void testXorLargeValues() {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        // Test with large floating point values to stress the compression
        long baseTime = 1000000000L;
        double[] values = {
            1.7976931348623157e+308,  // Near Double.MAX_VALUE
            -1.7976931348623157e+308, // Near -Double.MAX_VALUE
            4.9e-324,                 // Near Double.MIN_VALUE
            -4.9e-324,                // Near -Double.MIN_VALUE
            0.0,                      // Zero
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.NaN,               // Test NaN handling
            1234567890.123456789,     // Large precise value
            -9876543210.987654321,    // Large negative precise value
            Math.PI,                  // Irrational number
            Math.E                    // Another irrational number
        };

        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 1000, values[i]);
        }

        // Test chunk properties with large values
        assertEquals("Chunk should have all samples", values.length, chunk.numSamples());
        assertTrue("Chunk should have data", chunk.bytesSize() > 2);
        assertNotNull("BitStream should be accessible", chunk.getBitStream());

        // Verify all values can be read back correctly
        ChunkIterator iterator = chunk.iterator();
        int count = 0;

        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 1000, tv.timestamp());

            // For special values, use exact equality
            if (Double.isNaN(values[count])) {
                assertTrue("Expected NaN at index " + count, Double.isNaN(tv.value()));
            } else if (Double.isInfinite(values[count])) {
                assertEquals("Special value mismatch at index " + count, values[count], tv.value(), 0.0);
            } else {
                assertEquals("Value mismatch at index " + count, values[count], tv.value(), Math.abs(values[count]) * 1e-15);
            }
            count++;
        }

        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testXorTimestampJumps() {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        // Test with irregular timestamp patterns to stress timestamp compression
        long[] timestamps = {
            1000L,        // First value
            2000L,        // +1000 (normal delta)
            3000L,        // +1000 (same delta)
            10000L,       // +7000 (large jump)
            10100L,       // +100 (small delta)
            10050L,       // -50 (negative delta - should be rare but supported)
            50000L,       // +39950 (very large jump)
            50001L,       // +1 (very small delta)
            50002L,       // +1 (same small delta)
            100000L       // +49998 (another large jump)
        };

        double baseValue = 100.0;
        for (int i = 0; i < timestamps.length; i++) {
            appender.append(timestamps[i], baseValue + i);
        }

        // Verify all timestamps are preserved correctly
        ChunkIterator iterator = chunk.iterator();
        int count = 0;

        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, timestamps[count], tv.timestamp());
            assertEquals("Value mismatch at index " + count, baseValue + count, tv.value(), 0.001);
            count++;
        }

        assertEquals("Expected " + timestamps.length + " samples but got " + count, timestamps.length, count);
        assertNull("Iterator should not have errors", iterator.error());

        // Test multiple iterators work with irregular timestamps
        ChunkIterator iterator2 = chunk.iterator();
        assertNotNull("Should create independent iterator", iterator2);
    }

    public void testXorWithRepeatedValues() {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        // Add data with repeated values to test compression efficiency
        long baseTime = 2000000000L;
        double[] values = { 5.0, 5.0, 5.1, 5.1, 5.1, 5.2, 5.2, 5.0, 5.0, 5.3 };

        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 2000, values[i]);
        }

        // Verify decompression
        ChunkIterator iterator = chunk.iterator();
        int count = 0;

        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 2000, tv.timestamp());
            assertEquals("Value mismatch at index " + count, values[count], tv.value(), 0.000001);
            count++;
        }

        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testXorEmptyChunk() {
        XORChunk chunk = new XORChunk();

        assertEquals("Empty chunk should have 0 samples", 0, chunk.numSamples());
        assertEquals("Encoding should be XOR", Encoding.XOR, chunk.encoding());
        assertTrue("Empty chunk should have header bytes", chunk.bytesSize() > 0);
        assertNotNull("Empty chunk bytes should not be null", chunk.bytes());
        assertNotNull("BitStream should be accessible", chunk.getBitStream());

        ChunkIterator iterator = chunk.iterator();
        assertEquals("Empty chunk iterator should return NONE", ChunkIterator.ValueType.NONE, iterator.next());
        assertNull("Iterator should not have errors", iterator.error());

        // Test that we can get an appender for empty chunk
        ChunkAppender appender = chunk.appender();
        assertNotNull("Should be able to get appender for empty chunk", appender);

        // Test creating multiple iterators on empty chunk
        ChunkIterator iterator2 = chunk.iterator();
        assertNotNull("Should create new iterator instance", iterator2);
        assertEquals("New iterator should also return NONE", ChunkIterator.ValueType.NONE, iterator2.next());
    }

    public void testXorSingleValue() {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        appender.append(1000L, 42.0);

        assertEquals("Single value chunk should have 1 sample", 1, chunk.numSamples());

        ChunkIterator iterator = chunk.iterator();
        assertEquals("Should have one value", ChunkIterator.ValueType.FLOAT, iterator.next());

        ChunkIterator.TimestampValue tv = iterator.at();
        assertEquals("Timestamp should match", 1000L, tv.timestamp());
        assertEquals("Value should match", 42.0, tv.value(), 0.000001);

        assertEquals("Should be end of data", ChunkIterator.ValueType.NONE, iterator.next());
        assertNull("Iterator should not have errors", iterator.error());

        // Test appender restoration from single value
        ChunkAppender appender2 = chunk.appender();
        appender2.append(2000L, 84.0);
        assertEquals("Should have 2 samples after second appender", 2, chunk.numSamples());
    }

    public void testXorCompressionAndDecompression() {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        // Add some sample data
        long baseTime = 1000000000L; // Base timestamp
        double[] values = { 1.0, 1.1, 1.2, 1.15, 1.25, 1.3, 1.35, 1.4, 1.45, 1.5 };

        for (int i = 0; i < values.length; i++) {
            appender.append(baseTime + i * 1000, values[i]);
        }

        // Verify we can read the data back correctly
        ChunkIterator iterator = chunk.iterator();
        int count = 0;

        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertEquals("Timestamp mismatch at index " + count, baseTime + count * 1000, tv.timestamp());
            assertEquals("Value mismatch at index " + count, values[count], tv.value(), 0.000001);
            count++;
        }

        assertEquals("Expected " + values.length + " samples but got " + count, values.length, count);
        assertEquals("Chunk sample count mismatch", values.length, chunk.numSamples());
        assertEquals("Encoding should be XOR", Encoding.XOR, chunk.encoding());

        // Verify no errors occurred
        assertNull("Iterator should not have errors", iterator.error());
    }

    public void testXorWithDuplicateTimestamps() {
        // This test illustrates that XORChunk does NOT perform deduplication.
        // It stores all appended samples, including those with duplicate timestamps.
        // Deduplication is the responsibility of higher-level constructs like DedupIterator.

        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();

        // Append samples with some duplicate timestamps
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(2000L, 20.5);  // Duplicate timestamp, different value
        appender.append(3000L, 30.0);
        appender.append(3000L, 30.5);  // Duplicate timestamp, different value
        appender.append(3000L, 30.75); // Triple timestamp
        appender.append(4000L, 40.0);

        // XORChunk stores all samples including duplicates
        assertEquals("XORChunk stores all samples including duplicates", 7, chunk.numSamples());

        // Verify that iteration returns all samples in the order they were appended
        ChunkIterator iterator = chunk.iterator();

        long[] expectedTimestamps = { 1000L, 2000L, 2000L, 3000L, 3000L, 3000L, 4000L };
        double[] expectedValues = { 10.0, 20.0, 20.5, 30.0, 30.5, 30.75, 40.0 };

        int count = 0;
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            assertTrue("Too many samples", count < expectedTimestamps.length);

            assertEquals("Timestamp at index " + count, expectedTimestamps[count], tv.timestamp());
            assertEquals("Value at index " + count, expectedValues[count], tv.value(), 0.0);

            count++;
        }

        assertEquals("All samples should be readable", expectedTimestamps.length, count);
        assertNull("Iterator should not have errors", iterator.error());
    }
}
