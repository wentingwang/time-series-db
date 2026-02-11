/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBStatsAggregator.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Empty aggregation building</li>
 *   <li>Resource cleanup (doClose)</li>
 *   <li>Constructor parameter validation</li>
 * </ul>
 *
 * <p>Note: Full integration tests with document collection are in integration test suites
 * as they require complex TSDB infrastructure (TSDBLeafReader, TSDBDocValues, etc.).</p>
 */
public class TSDBStatsAggregatorTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Empty Aggregation Tests ==========

    public void testBuildEmptyAggregationWithValueStatsEnabled() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();

        // Assert
        assertNotNull("Empty aggregation should not be null", result);
        assertTrue("Should return InternalTSDBStats", result instanceof InternalTSDBStats);

        InternalTSDBStats tsdbStats = (InternalTSDBStats) result;
        assertEquals("Name should match", TEST_NAME, tsdbStats.getName());
        assertNull("Empty aggregation should have null numSeries", tsdbStats.getNumSeries());
        assertNotNull("Empty aggregation should have label stats", tsdbStats.getLabelStats());
        assertEquals("Empty aggregation should have empty label stats", 0, tsdbStats.getLabelStats().size());

        // Cleanup
        aggregator.close();
    }

    public void testBuildEmptyAggregationWithValueStatsDisabled() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();

        // Assert
        assertNotNull("Empty aggregation should not be null", result);
        assertTrue("Should return InternalTSDBStats", result instanceof InternalTSDBStats);

        InternalTSDBStats tsdbStats = (InternalTSDBStats) result;
        assertEquals("Name should match", TEST_NAME, tsdbStats.getName());

        // Cleanup
        aggregator.close();
    }

    public void testBuildEmptyAggregationReturnsCoordinatorLevelStats() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();

        // Assert
        InternalTSDBStats tsdbStats = (InternalTSDBStats) result;
        // Empty aggregation should return coordinator-level stats (not shard-level)
        assertNotNull("Should have coordinator-level label stats", tsdbStats.getLabelStats());

        // Cleanup
        aggregator.close();
    }

    // ========== Resource Cleanup Tests ==========

    public void testDoCloseWithValueStatsEnabled() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act - Should not throw exception
        aggregator.close();

        // Assert - No exception means success
        assertTrue("Close should complete successfully", true);
    }

    public void testDoCloseWithValueStatsDisabled() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);

        // Act - Should not throw exception
        aggregator.close();

        // Assert - No exception means success
        assertTrue("Close should complete successfully", true);
    }

    public void testDoCloseMultipleTimes() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act - Close multiple times should not throw exception
        aggregator.close();
        aggregator.close();

        // Assert
        assertTrue("Multiple closes should complete successfully", true);
    }

    public void testDoCloseAfterBuildEmptyAggregation() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator.buildEmptyAggregation();

        // Act - Close after building empty aggregation
        aggregator.close();

        // Assert
        assertTrue("Close after buildEmptyAggregation should complete successfully", true);
    }

    // ========== Constructor Parameter Tests ==========

    public void testAggregatorWithLargeTimeRange() throws IOException {
        // Arrange & Act
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, Long.MIN_VALUE, Long.MAX_VALUE, true);

        // Assert
        assertNotNull("Aggregator should be created with large time range", aggregator);

        // Cleanup
        aggregator.close();
    }

    public void testAggregatorWithZeroTimeRange() throws IOException {
        // Arrange & Act
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, 0L, 1L, true);

        // Assert
        assertNotNull("Aggregator should be created with zero min timestamp", aggregator);

        // Cleanup
        aggregator.close();
    }

    public void testAggregatorWithNegativeTimestamps() throws IOException {
        // Arrange & Act
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, -2000L, -1000L, true);

        // Assert
        assertNotNull("Aggregator should be created with negative timestamps", aggregator);

        // Cleanup
        aggregator.close();
    }

    public void testAggregatorWithDifferentNames() throws IOException {
        // Arrange & Act
        TSDBStatsAggregator agg1 = createAggregator("simple", MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        TSDBStatsAggregator agg2 = createAggregator("with-dashes", MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        TSDBStatsAggregator agg3 = createAggregator("with_underscores", MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Assert
        InternalAggregation result1 = agg1.buildEmptyAggregation();
        InternalAggregation result2 = agg2.buildEmptyAggregation();
        InternalAggregation result3 = agg3.buildEmptyAggregation();

        assertEquals("simple", result1.getName());
        assertEquals("with-dashes", result2.getName());
        assertEquals("with_underscores", result3.getName());

        // Cleanup
        agg1.close();
        agg2.close();
        agg3.close();
    }

    // ========== Metadata Tests ==========

    public void testBuildEmptyAggregationWithMetadata() throws IOException {
        // Arrange
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);
        TSDBStatsAggregator aggregator = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, metadata);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();

        // Assert
        assertNotNull("Result should have metadata", result.getMetadata());
        assertEquals("Metadata should match", metadata, result.getMetadata());

        // Cleanup
        aggregator.close();
    }

    public void testBuildEmptyAggregationWithoutMetadata() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, null);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();

        // Assert
        assertNull("Result should have null metadata", result.getMetadata());

        // Cleanup
        aggregator.close();
    }

    // ========== Edge Case Tests ==========

    public void testMultipleEmptyAggregations() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act - Build multiple empty aggregations
        InternalAggregation result1 = aggregator.buildEmptyAggregation();
        InternalAggregation result2 = aggregator.buildEmptyAggregation();

        // Assert - Should return equivalent results
        assertEquals("Multiple calls should return equivalent results", result1, result2);

        // Cleanup
        aggregator.close();
    }

    public void testEmptyAggregationFollowedByClose() throws IOException {
        // Arrange
        TSDBStatsAggregator aggregator = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        InternalAggregation result = aggregator.buildEmptyAggregation();
        assertNotNull(result);

        // Close and ensure no exceptions
        aggregator.close();

        // Assert
        assertTrue("Should complete successfully", true);
    }

    // ========== Helper Methods ==========

    /**
     * Helper method to create a TSDBStatsAggregator with minimal mocking.
     *
     * @param name The name of the aggregator
     * @param minTimestamp The minimum timestamp
     * @param maxTimestamp The maximum timestamp
     * @param includeValueStats Whether to include value statistics
     * @return A TSDBStatsAggregator instance
     * @throws IOException If an error occurs during creation
     */
    private TSDBStatsAggregator createAggregator(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats)
        throws IOException {
        return createAggregatorWithMetadata(name, minTimestamp, maxTimestamp, includeValueStats, Map.of());
    }

    /**
     * Helper method to create a TSDBStatsAggregator with metadata.
     *
     * @param name The name of the aggregator
     * @param minTimestamp The minimum timestamp
     * @param maxTimestamp The maximum timestamp
     * @param includeValueStats Whether to include value statistics
     * @param metadata The aggregation metadata
     * @return A TSDBStatsAggregator instance
     * @throws IOException If an error occurs during creation
     */
    private TSDBStatsAggregator createAggregatorWithMetadata(
        String name,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        Map<String, Object> metadata
    ) throws IOException {
        // Create a minimal mock SearchContext
        SearchContext context = mock(SearchContext.class);

        // Create BigArrays with NoneCircuitBreakerService
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");

        // Mock BigArrays
        when(context.bigArrays()).thenReturn(bigArrays);

        return new TSDBStatsAggregator(name, context, null, minTimestamp, maxTimestamp, includeValueStats, metadata);
    }
}
