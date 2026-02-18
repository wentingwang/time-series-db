/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBStatsAggregatorFactory.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Factory configuration with various parameters</li>
 *   <li>Concurrent segment search support</li>
 *   <li>Edge cases (extreme timestamps, boolean flags)</li>
 * </ul>
 */
public class TSDBStatsAggregatorFactoryTests extends OpenSearchTestCase {

    public void testSupportsConcurrentSegmentSearch() {
        // Arrange
        TSDBStatsAggregatorFactory factory = createFactory("test_css", 1000L, 2000L, true);

        // Act & Assert
        assertTrue("TSDBStatsAggregatorFactory should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testSupportsConcurrentSegmentSearchWithValueStatsDisabled() {
        // Arrange
        TSDBStatsAggregatorFactory factory = createFactory("test_css_no_values", 1000L, 2000L, false);

        // Act & Assert
        assertTrue(
            "TSDBStatsAggregatorFactory should support CSS regardless of includeValueStats",
            factory.supportsConcurrentSegmentSearch()
        );
    }

    public void testFactoryConfiguration() {
        // Arrange
        long minTimestamp = 5000L;
        long maxTimestamp = 10000L;
        boolean includeValueStats = true;

        // Act
        TSDBStatsAggregatorFactory factory = createFactory("config_test", minTimestamp, maxTimestamp, includeValueStats);

        // Assert - We can't directly access private fields, but we can verify the factory was created successfully
        assertNotNull("Factory should be created successfully", factory);
        assertEquals("config_test", factory.name());
    }

    public void testFactoryWithLargeTimeRange() {
        // Arrange - Test with large timestamp values
        long minTimestamp = Long.MIN_VALUE;
        long maxTimestamp = Long.MAX_VALUE;

        // Act
        TSDBStatsAggregatorFactory factory = createFactory("large_range", minTimestamp, maxTimestamp, true);

        // Assert
        assertNotNull("Factory should handle large timestamp values", factory);
        assertTrue("Should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testFactoryWithZeroTimeRange() {
        // Arrange - Test edge case with zero timestamps
        long timestamp = 0L;

        // Act
        TSDBStatsAggregatorFactory factory = createFactory("zero_time", timestamp, timestamp, false);

        // Assert
        assertNotNull("Factory should handle zero timestamps", factory);
    }

    public void testFactoryWithNegativeTimeRange() {
        // Arrange - Test with negative timestamps (valid for epoch times)
        long minTimestamp = -1000L;
        long maxTimestamp = -500L;

        // Act
        TSDBStatsAggregatorFactory factory = createFactory("negative_time", minTimestamp, maxTimestamp, true);

        // Assert
        assertNotNull("Factory should handle negative timestamps", factory);
    }

    /**
     * Tests that factory properly initializes with includeValueStats enabled.
     */
    public void testFactoryWithValueStatsEnabled() {
        // Arrange & Act
        TSDBStatsAggregatorFactory factory = createFactory("with_values", 1000L, 2000L, true);

        // Assert
        assertNotNull("Factory should be created with value stats enabled", factory);
    }

    /**
     * Tests that factory properly initializes with includeValueStats disabled.
     */
    public void testFactoryWithValueStatsDisabled() {
        // Arrange & Act
        TSDBStatsAggregatorFactory factory = createFactory("without_values", 1000L, 2000L, false);

        // Assert
        assertNotNull("Factory should be created with value stats disabled", factory);
    }

    /**
     * Tests that factory can be created with all edge case parameters.
     */
    public void testFactoryWithEdgeCaseParameters() {
        // Arrange & Act - Min timestamps, max timestamps, both boolean values
        TSDBStatsAggregatorFactory factory1 = createFactory("edge_case_1", Long.MIN_VALUE, Long.MAX_VALUE, true);
        TSDBStatsAggregatorFactory factory2 = createFactory("edge_case_2", 0L, 0L, false);

        // Assert
        assertNotNull("Factory should handle min/max edge cases", factory1);
        assertNotNull("Factory should handle zero edge cases", factory2);
        assertTrue("Should support CSS", factory1.supportsConcurrentSegmentSearch());
        assertTrue("Should support CSS", factory2.supportsConcurrentSegmentSearch());
    }

    /**
     * Tests that factory name is properly stored.
     */
    public void testFactoryName() {
        // Arrange
        String expectedName = "my_tsdb_stats";

        // Act
        TSDBStatsAggregatorFactory factory = createFactory(expectedName, 1000L, 2000L, true);

        // Assert
        assertEquals("Factory name should match", expectedName, factory.name());
    }

    /**
     * Tests factory with various name patterns.
     */
    public void testFactoryWithVariousNames() {
        // Arrange & Act
        TSDBStatsAggregatorFactory factory1 = createFactory("simple", 1000L, 2000L, true);
        TSDBStatsAggregatorFactory factory2 = createFactory("with-dashes", 1000L, 2000L, true);
        TSDBStatsAggregatorFactory factory3 = createFactory("with_underscores", 1000L, 2000L, true);
        TSDBStatsAggregatorFactory factory4 = createFactory("with.dots", 1000L, 2000L, true);

        // Assert
        assertEquals("simple", factory1.name());
        assertEquals("with-dashes", factory2.name());
        assertEquals("with_underscores", factory3.name());
        assertEquals("with.dots", factory4.name());
    }

    /**
     * Tests that createInternal() properly creates a TSDBStatsAggregator instance.
     */
    public void testCreateInternal() throws Exception {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        boolean includeValueStats = true;
        TSDBStatsAggregatorFactory factory = createFactory("test_create", minTimestamp, maxTimestamp, includeValueStats);

        // Create mock SearchContext
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,  // parent aggregator
            CardinalityUpperBound.NONE,
            Map.of()  // metadata
        );

        // Assert
        assertNotNull("createInternal() should return a TSDBStatsAggregator instance", aggregator);
        assertEquals("Aggregator name should match factory name", "test_create", aggregator.name());

        // Cleanup
        aggregator.close();
    }

    /**
     * Tests that createInternal() works with different parameter combinations.
     */
    public void testCreateInternalWithDifferentParameters() throws Exception {
        // Arrange - Test with includeValueStats=false
        TSDBStatsAggregatorFactory factory = createFactory("test_no_values", 2000L, 6000L, false);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.NONE,
            Map.of("custom_meta", "value")
        );

        // Assert
        assertNotNull("Should create aggregator with value stats disabled", aggregator);
        assertEquals("test_no_values", aggregator.name());

        // Cleanup
        aggregator.close();
    }

    /**
     * Tests that createInternal() works with extreme timestamp ranges.
     */
    public void testCreateInternalWithExtremeTimestamps() throws Exception {
        // Arrange
        TSDBStatsAggregatorFactory factory = createFactory("test_extreme", Long.MIN_VALUE, Long.MAX_VALUE, true);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.NONE,
            Map.of()
        );

        // Assert
        assertNotNull("Should create aggregator with extreme timestamps", aggregator);

        // Cleanup
        aggregator.close();
    }

    /**
     * Helper method to create TSDBStatsAggregatorFactory with minimal parameters.
     * Uses null for complex OpenSearch infrastructure components that aren't needed for basic tests.
     */
    private TSDBStatsAggregatorFactory createFactory(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats) {
        try {
            // Create an empty AggregatorFactories.Builder to satisfy the constructor
            AggregatorFactories.Builder subFactoriesBuilder = new AggregatorFactories.Builder();

            return new TSDBStatsAggregatorFactory(
                name,
                null, // QueryShardContext - not needed for basic tests
                null, // AggregatorFactory parent - not needed
                subFactoriesBuilder, // AggregatorFactories.Builder - required by parent constructor
                Map.of(), // metadata
                minTimestamp,
                maxTimestamp,
                includeValueStats
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create factory for test", e);
        }
    }
}
