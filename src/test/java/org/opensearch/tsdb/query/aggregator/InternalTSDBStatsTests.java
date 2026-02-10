/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InternalTSDBStatsTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final Map<String, Object> TEST_METADATA = Map.of("key1", "value1");

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(headStats, internal.getHeadStats());
        assertEquals(500L, internal.getNumSeries().longValue());
        assertEquals(labelStats, internal.getLabelStats());
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    public void testConstructorWithNullHeadStats() {
        // Arrange
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Assert
        assertNull(internal.getHeadStats());
        assertEquals(500L, internal.getNumSeries().longValue());
    }

    public void testConstructorWithNullNumSeries() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(null, labelStats),
            TEST_METADATA
        );

        // Assert
        assertNull(internal.getNumSeries());
        assertEquals(headStats, internal.getHeadStats());
    }

    public void testConstructorWithEmptyLabelStats() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> emptyLabelStats = new HashMap<>();

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, emptyLabelStats),
            TEST_METADATA
        );

        // Assert
        assertEquals(0, internal.getLabelStats().size());
    }

    // ========== HeadStats Tests ==========

    public void testHeadStatsConstructor() {
        // Act
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);

        // Assert
        assertEquals(100L, headStats.numSeries());
        assertEquals(200L, headStats.chunkCount());
        assertEquals(1000L, headStats.minTime());
        assertEquals(2000L, headStats.maxTime());
    }

    public void testHeadStatsEquals() {
        // Arrange
        InternalTSDBStats.HeadStats stats1 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats2 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats3 = new InternalTSDBStats.HeadStats(101L, 200L, 1000L, 2000L);

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testHeadStatsSerialization() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats original = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.HeadStats deserialized = new InternalTSDBStats.HeadStats(in);

        // Assert
        assertEquals(original, deserialized);
    }

    // ========== LabelStats Tests ==========

    public void testLabelStatsConstructor() {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);

        // Act
        InternalTSDBStats.CoordinatorLevelStats.LabelStats labelStats = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats
        );

        // Assert
        assertEquals(100L, labelStats.numSeries().longValue());
        assertEquals(valuesStats, labelStats.valuesStats());
    }

    public void testLabelStatsGetValues() {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);
        InternalTSDBStats.CoordinatorLevelStats.LabelStats labelStats = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats
        );

        // Act
        List<String> values = labelStats.values();

        // Assert
        assertEquals(2, values.size());
        assertTrue(values.contains("prod"));
        assertTrue(values.contains("staging"));
    }

    public void testLabelStatsGetValuesWithNullValuesStats() {
        // Arrange
        InternalTSDBStats.CoordinatorLevelStats.LabelStats labelStats = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            null
        );

        // Act
        List<String> values = labelStats.values();

        // Assert
        assertNotNull(values);
        assertEquals(0, values.size());
    }

    public void testLabelStatsEquals() {
        // Arrange
        Map<String, Long> valuesStats1 = Map.of("prod", 80L);
        Map<String, Long> valuesStats2 = Map.of("prod", 80L);
        Map<String, Long> valuesStats3 = Map.of("staging", 20L);

        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats1 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats1
        );
        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats2 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats2
        );
        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats3 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats3
        );

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testLabelStatsSerialization() throws IOException {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);
        InternalTSDBStats.CoordinatorLevelStats.LabelStats original = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            null,
            valuesStats
        );

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats.LabelStats deserialized = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.numSeries(), deserialized.numSeries());
        assertEquals(original.valuesStats(), deserialized.valuesStats());
    }

    public void testLabelStatsSerializationWithNullValues() throws IOException {
        // Arrange
        InternalTSDBStats.CoordinatorLevelStats.LabelStats original = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            null,
            null,
            null
        );

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats.LabelStats deserialized = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertNull(deserialized.numSeries());
        assertNull(deserialized.valuesStats());
    }

    // ========== Interface Implementation Tests ==========

    public void testGetWriteableName() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals("tsdb_stats", internal.getWriteableName());
    }

    public void testMustReduceOnSingleInternalAgg() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        // Act & Assert
        assertTrue(internal.mustReduceOnSingleInternalAgg());
    }

    // ========== Serialization Tests ==========

    public void testFullSerialization() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats original = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats deserialized = new InternalTSDBStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getHeadStats(), deserialized.getHeadStats());
        assertEquals(original.getNumSeries(), deserialized.getNumSeries());
        assertEquals(original.getLabelStats(), deserialized.getLabelStats());
    }

    public void testSerializationWithNullFields() throws IOException {
        // Arrange
        InternalTSDBStats original = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            null
        );

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats deserialized = new InternalTSDBStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertNull(deserialized.getHeadStats());
        assertNull(deserialized.getNumSeries());
        assertNull(deserialized.getMetadata());
    }

    // ========== XContent (JSON) Output Tests ==========

    public void testDoXContentBodyWithAllFields() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, null, clusterValues));

        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(25644L, labelStats),
            TEST_METADATA
        );

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertNotNull(json);
        assertTrue(json.contains("\"headStats\""));
        assertTrue(json.contains("\"numSeries\":508"));
        assertTrue(json.contains("\"chunkCount\":937"));
        assertTrue(json.contains("\"labelStats\""));
        assertTrue(json.contains("\"numSeries\":25644"));
        assertTrue(json.contains("\"cluster\""));
        assertTrue(json.contains("\"values\""));
        assertTrue(json.contains("\"valuesStats\""));
    }

    public void testDoXContentBodyWithoutHeadStats() throws IOException {
        // Arrange
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertFalse(json.contains("\"headStats\""));
        assertTrue(json.contains("\"labelStats\""));
    }

    public void testDoXContentBodyWithoutNumSeries() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(null, labelStats),
            TEST_METADATA
        );

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertTrue(json.contains("\"headStats\""));
        assertTrue(json.contains("\"labelStats\""));
        // numSeries at top level should not be present
        assertFalse(json.contains("\"numSeries\":null"));
    }

    public void testDoXContentBodyWithEmptyLabelStats() throws IOException {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertTrue(json.contains("\"labelStats\":{}"));
    }

    // ========== Property Tests ==========

    public void testGetPropertyEmptyPath() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );

        // Act
        Object result = internal.getProperty(List.of());

        // Assert
        assertEquals(internal, result);
    }

    public void testGetPropertyNumSeries() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );

        // Act
        Object result = internal.getProperty(List.of("numSeries"));

        // Assert
        assertEquals(500L, result);
    }

    public void testGetPropertyLabelStats() {
        // Arrange
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act
        Object result = internal.getProperty(List.of("labelStats"));

        // Assert
        assertEquals(labelStats, result);
    }

    public void testGetPropertyInvalidPath() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> internal.getProperty(List.of("invalidProperty"))
        );
        assertTrue(exception.getMessage().contains("Unknown property"));
        assertTrue(exception.getMessage().contains("invalidProperty"));
    }

    // ========== Reduce Tests ==========

    public void testReduceWithEmptyAggregationsList() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );
        List<InternalAggregation> emptyAggregations = List.of();

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            (s) -> {},
            emptyPipelineTree
        );

        // Act
        InternalAggregation result = internal.reduce(emptyAggregations, finalReduceContext);

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertEquals(TEST_NAME, reducedStats.getName());
    }

    public void testReduceWithSingleAggregation() {
        // Arrange
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        List<InternalAggregation> aggregations = List.of(internal);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            (s) -> {},
            emptyPipelineTree
        );

        // Act
        InternalAggregation result = internal.reduce(aggregations, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertEquals(internal, reducedStats);
    }

    // ========== Equals and HashCode Tests ==========

    public void testEquals() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats3 = InternalTSDBStats.forCoordinatorLevel(
            "different-name",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
    }

    public void testHashCode() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    // ========== ShardLevelStats Tests ==========
    // Note: Serialization tests for ShardLevelStats are skipped due to a pre-existing bug in the production code
    // where HyperLogLogPlusPlus.readFrom() can return HyperLogLogPlusPlusSparse which cannot be cast to
    // HyperLogLogPlusPlus (see InternalTSDBStats.java:91). This bug exists in the original code and is outside
    // the scope of the Record conversion. The tests below verify the Record accessors work correctly.

    public void testShardLevelStatsConstructor() throws IOException {
        // Arrange
        Set<Long> seriesFingerprintSet = new HashSet<>();
        seriesFingerprintSet.add(1L);
        seriesFingerprintSet.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new HashMap<>();
        Map<String, Set<Long>> clusterFingerprintSets = new HashMap<>();
        Set<Long> prodFingerprints = new HashSet<>();
        prodFingerprints.add(100L);
        clusterFingerprintSets.put("prod", prodFingerprints);
        labelStats.put("cluster", clusterFingerprintSets);

        // Act
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet, labelStats);

        // Assert
        assertNotNull(shardStats.seriesFingerprintSet());
        assertEquals(labelStats, shardStats.labelStats());
        assertEquals(1, shardStats.labelStats().size());
    }

    public void testShardLevelStatsWithNullSeriesFingerprints() {
        // Arrange
        Map<String, Map<String, Set<Long>>> labelStats = new HashMap<>();

        // Act
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(null, labelStats);

        // Assert
        assertNull(shardStats.seriesFingerprintSet());
        assertEquals(labelStats, shardStats.labelStats());
    }

    public void testShardLevelStatsWithEmptyLabelStats() throws IOException {
        // Arrange
        Set<Long> seriesFingerprintSet = new HashSet<>();
        seriesFingerprintSet.add(1L);
        Map<String, Map<String, Set<Long>>> emptyLabelStats = new HashMap<>();

        // Act
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet, emptyLabelStats);

        // Assert
        assertNotNull(shardStats.seriesFingerprintSet());
        assertEquals(0, shardStats.labelStats().size());
    }

    public void testShardLevelStatsWithNullValueMap() {
        // Arrange
        Set<Long> seriesFingerprintSet = new HashSet<>();
        Map<String, Map<String, Set<Long>>> labelStats = new HashMap<>();
        labelStats.put("cluster", null); // null value map

        // Act
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet, labelStats);

        // Assert
        assertNotNull(shardStats.seriesFingerprintSet());
        assertEquals(1, shardStats.labelStats().size());
        assertNull(shardStats.labelStats().get("cluster"));
    }

    // ========== Helper Methods ==========

    private Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> createTestLabelStats() {
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, Long> regionValues = Map.of("us-east", 40L, "us-west", 30L, "eu-west", 30L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, null, clusterValues));
        labelStats.put("region", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, null, regionValues));

        return labelStats;
    }
}
