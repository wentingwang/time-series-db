/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal aggregation result for TSDB statistics.
 *
 * <p>This class represents the result of TSDB statistics aggregations, containing
 * statistics about label keys and values across time series data.</p>
 *
 * <h2>Two-Phase Reduce Strategy:</h2>
 * <p>This class uses different data structures for different reduce phases:</p>
 * <ul>
 *   <li><strong>Shard-level phase:</strong> Uses {@link ShardLevelStats} with HLL++ sketches
 *       to deduplicate time series between Head and ClosedChunkIndex within a shard</li>
 *   <li><strong>Coordinator-level phase:</strong> Uses {@link CoordinatorLevelStats} with final
 *       counts summed from different shards (no deduplication needed due to routing)</li>
 * </ul>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Total Time Series Count:</strong> Total number of unique time series</li>
 *   <li><strong>Tag Statistics:</strong> Per-tag cardinality and value distribution</li>
 *   <li><strong>Optional Value Stats:</strong> Detailed per-value counts when enabled</li>
 *   <li><strong>Memory Efficient:</strong> HLL sketches discarded after shard-level reduce
 *       to minimize network bandwidth</li>
 * </ul>
 */
public class InternalTSDBStats extends InternalAggregation {

    private final HeadStats headStats;

    // Exactly one of these will be non-null to indicate which phase we're in
    private final ShardLevelStats shardStats;
    private final CoordinatorLevelStats coordinatorStats;

    /**
     * Statistics for the head (in-memory time series).
     */
    public record HeadStats(long numSeries, long chunkCount, long minTime, long maxTime) {

        public HeadStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(numSeries);
            out.writeVLong(chunkCount);
            out.writeVLong(minTime);
            out.writeVLong(maxTime);
        }
    }

    /**
     * Shard-level statistics containing HLL++ sketches.
     *
     * <p>Used during shard-level aggregation and reduce to deduplicate time series
     * between Head and ClosedChunkIndex. Converted to {@link CoordinatorLevelStats}
     * after shard-level reduce to save network bandwidth.</p>
     */
    public static class ShardLevelStats {
        private final HyperLogLogPlusPlus seriesCardinalitySketch;
        // Map of label name -> Map of label value -> HLL sketch
        private final Map<String, Map<String, HyperLogLogPlusPlus>> labelStats;

        public ShardLevelStats(HyperLogLogPlusPlus seriesCardinalitySketch, Map<String, Map<String, HyperLogLogPlusPlus>> labelStats) {
            this.seriesCardinalitySketch = seriesCardinalitySketch;
            this.labelStats = labelStats;
        }

        public ShardLevelStats(StreamInput in) throws IOException {
            boolean hasSketch = in.readBoolean();
            this.seriesCardinalitySketch = hasSketch
                ? (HyperLogLogPlusPlus) HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE)
                : null;

            int labelCount = in.readVInt();
            this.labelStats = new HashMap<>(labelCount);
            for (int i = 0; i < labelCount; i++) {
                String labelName = in.readString();

                // Read value sketches for this label
                boolean hasSketches = in.readBoolean();
                Map<String, HyperLogLogPlusPlus> valueCardinalitySketches;
                if (hasSketches) {
                    int mapSize = in.readVInt();
                    valueCardinalitySketches = new LinkedHashMap<>(mapSize);
                    for (int j = 0; j < mapSize; j++) {
                        String key = in.readString();
                        // Read whether this value has a sketch (null when includeValueStats=false)
                        boolean hasValueSketch = in.readBoolean();
                        HyperLogLogPlusPlus sketch;
                        if (hasValueSketch) {
                            sketch = (HyperLogLogPlusPlus) HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
                        } else {
                            sketch = null;
                        }
                        valueCardinalitySketches.put(key, sketch);
                    }
                } else {
                    valueCardinalitySketches = null;
                }

                this.labelStats.put(labelName, valueCardinalitySketches);
            }
        }

        /**
         * Serializes LabelStats to a stream.
         *
         * @param out the stream output to write to
         * @throws IOException if an I/O error occurs during writing
         */
        public void writeTo(StreamOutput out) throws IOException {
            if (seriesCardinalitySketch != null) {
                out.writeBoolean(true);
                seriesCardinalitySketch.writeTo(0, out);
            } else {
                out.writeBoolean(false);
            }

            out.writeVInt(labelStats.size());
            for (Map.Entry<String, Map<String, HyperLogLogPlusPlus>> entry : labelStats.entrySet()) {
                out.writeString(entry.getKey());

                // Write value sketches for this label
                Map<String, HyperLogLogPlusPlus> valueCardinalitySketches = entry.getValue();
                if (valueCardinalitySketches != null) {
                    out.writeBoolean(true);
                    out.writeVInt(valueCardinalitySketches.size());
                    for (Map.Entry<String, HyperLogLogPlusPlus> ve : valueCardinalitySketches.entrySet()) {
                        out.writeString(ve.getKey());
                        // Write whether this value has a sketch (null when includeValueStats=false)
                        HyperLogLogPlusPlus sketch = ve.getValue();
                        if (sketch != null) {
                            out.writeBoolean(true);
                            sketch.writeTo(0, out);
                        } else {
                            out.writeBoolean(false);
                        }
                    }
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        public HyperLogLogPlusPlus getSeriesCardinalitySketch() {
            return seriesCardinalitySketch;
        }

        public Map<String, Map<String, HyperLogLogPlusPlus>> getLabelStats() {
            return labelStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardLevelStats that = (ShardLevelStats) o;
            return Objects.equals(seriesCardinalitySketch, that.seriesCardinalitySketch) && Objects.equals(labelStats, that.labelStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seriesCardinalitySketch, labelStats);
        }
    }

    /**
     * Coordinator-level statistics containing final counts.
     *
     * <p>Used after shard-level reduce when aggregating results from multiple shards.
     * Contains pre-computed cardinality counts instead of HLL sketches to minimize
     * network bandwidth (1000x smaller than sketches).</p>
     */
    public static class CoordinatorLevelStats {
        private final Long totalNumSeries;
        private final Map<String, LabelStats> labelStats;

        public CoordinatorLevelStats(Long totalNumSeries, Map<String, LabelStats> labelStats) {
            this.totalNumSeries = totalNumSeries;
            this.labelStats = labelStats;
        }

        public CoordinatorLevelStats(StreamInput in) throws IOException {
            boolean hasNumSeries = in.readBoolean();
            this.totalNumSeries = hasNumSeries ? in.readVLong() : null;

            int labelCount = in.readVInt();
            this.labelStats = new HashMap<>(labelCount);
            for (int i = 0; i < labelCount; i++) {
                String labelName = in.readString();
                LabelStats stats = new LabelStats(in);
                this.labelStats.put(labelName, stats);
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
            if (totalNumSeries != null) {
                out.writeBoolean(true);
                out.writeVLong(totalNumSeries);
            } else {
                out.writeBoolean(false);
            }

            out.writeVInt(labelStats.size());
            for (Map.Entry<String, LabelStats> entry : labelStats.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }

        public Long getTotalNumSeries() {
            return totalNumSeries;
        }

        public Map<String, LabelStats> getLabelStats() {
            return labelStats;
        }

        /**
         * Coordinator-level label statistics with final counts.
         */
        public static class LabelStats {
            private final Long numSeries;
            private final List<String> values; // List of label values (always populated)
            private final Map<String, Long> valuesStats; // Cardinality per value (only when includeValueStats=true)

            public LabelStats(Long numSeries, List<String> values, Map<String, Long> valuesStats) {
                this.numSeries = numSeries;
                this.values = values != null ? values : (valuesStats != null ? new ArrayList<>(valuesStats.keySet()) : List.of());
                this.valuesStats = valuesStats;
            }

            public LabelStats(StreamInput in) throws IOException {
                boolean hasNumSeries = in.readBoolean();
                this.numSeries = hasNumSeries ? in.readVLong() : null;

                // Read values list
                this.values = in.readStringList();

                boolean hasValuesStats = in.readBoolean();
                if (hasValuesStats) {
                    this.valuesStats = in.readMap(StreamInput::readString, StreamInput::readVLong);
                } else {
                    this.valuesStats = null;
                }
            }

            public void writeTo(StreamOutput out) throws IOException {
                if (numSeries != null) {
                    out.writeBoolean(true);
                    out.writeVLong(numSeries);
                } else {
                    out.writeBoolean(false);
                }

                // Write values list
                out.writeStringCollection(values);

                if (valuesStats != null) {
                    out.writeBoolean(true);
                    out.writeMap(valuesStats, StreamOutput::writeString, StreamOutput::writeVLong);
                } else {
                    out.writeBoolean(false);
                }
            }

            public Long getNumSeries() {
                return numSeries;
            }

            public List<String> getValues() {
                return values;
            }

            public Map<String, Long> getValuesStats() {
                return valuesStats;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                LabelStats that = (LabelStats) o;
                return Objects.equals(numSeries, that.numSeries)
                    && Objects.equals(values, that.values)
                    && Objects.equals(valuesStats, that.valuesStats);
            }

            @Override
            public int hashCode() {
                return Objects.hash(numSeries, values, valuesStats);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorLevelStats that = (CoordinatorLevelStats) o;
            return Objects.equals(totalNumSeries, that.totalNumSeries) && Objects.equals(labelStats, that.labelStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalNumSeries, labelStats);
        }
    }

    /**
     * Factory method for creating shard-level stats (with HLL sketches).
     *
     * @param name the name of the aggregation
     * @param shardStats the shard-level statistics with HLL sketches
     * @param metadata the aggregation metadata
     * @return InternalTSDBStats instance for shard-level phase
     */
    public static InternalTSDBStats forShardLevel(String name, ShardLevelStats shardStats, Map<String, Object> metadata) {
        return new InternalTSDBStats(name, null, shardStats, null, metadata);
    }

    /**
     * Factory method for creating coordinator-level stats (with final counts).
     *
     * @param name the name of the aggregation
     * @param headStats the head statistics (null if not populated)
     * @param coordinatorStats the coordinator-level statistics with final counts
     * @param metadata the aggregation metadata
     * @return InternalTSDBStats instance for coordinator-level phase
     */
    public static InternalTSDBStats forCoordinatorLevel(
        String name,
        HeadStats headStats,
        CoordinatorLevelStats coordinatorStats,
        Map<String, Object> metadata
    ) {
        return new InternalTSDBStats(name, headStats, null, coordinatorStats, metadata);
    }

    /**
     * Private constructor - use factory methods instead.
     */
    private InternalTSDBStats(
        String name,
        HeadStats headStats,
        ShardLevelStats shardStats,
        CoordinatorLevelStats coordinatorStats,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.headStats = headStats;
        this.shardStats = shardStats;
        this.coordinatorStats = coordinatorStats;

        // Sanity check: exactly one of shardStats or coordinatorStats must be non-null
        if ((shardStats == null) == (coordinatorStats == null)) {
            throw new IllegalArgumentException("Exactly one of shardStats or coordinatorStats must be non-null");
        }
    }

    /**
     * Reads an InternalTSDBStats from a stream for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTSDBStats(StreamInput in) throws IOException {
        super(in);

        // Read headStats if present
        boolean hasHeadStats = in.readBoolean();
        this.headStats = hasHeadStats ? new HeadStats(in) : null;

        // Read which mode we're in
        boolean isShardLevel = in.readBoolean();
        if (isShardLevel) {
            this.shardStats = new ShardLevelStats(in);
            this.coordinatorStats = null;
        } else {
            this.shardStats = null;
            this.coordinatorStats = new CoordinatorLevelStats(in);
        }
    }

    /**
     * Writes the InternalTSDBStats data to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Write headStats if present
        if (headStats != null) {
            out.writeBoolean(true);
            headStats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        // Write which mode we're in and the corresponding stats
        if (shardStats != null) {
            out.writeBoolean(true); // isShardLevel = true
            shardStats.writeTo(out);
        } else {
            out.writeBoolean(false); // isShardLevel = false
            coordinatorStats.writeTo(out);
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "tsdb_stats"
     */
    @Override
    public String getWriteableName() {
        return "tsdb_stats";
    }

    /**
     * Reduces multiple InternalTSDBStats aggregations into a single result.
     *
     * <p>This method uses different strategies based on the reduce phase:</p>
     * <ul>
     * <li><b>Shard-level reduce (!isFinalReduce):</b> Merges HLL sketches to deduplicate
     *     time series between Head and ClosedChunkIndex within the same shard. Converts
     *     sketches to counts and returns {@link CoordinatorLevelStats} to save network bandwidth.</li>
     * <li><b>Coordinator reduce (isFinalReduce):</b> Simply sums counts from different shards
     *     since each time series is guaranteed to exist on only one shard (by routing).</li>
     * </ul>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (aggregations.isEmpty()) {
            // Return empty coordinator-level stats
            return forCoordinatorLevel(name, null, new CoordinatorLevelStats(null, new HashMap<>()), metadata);
        }

        if (!reduceContext.isFinalReduce()) {
            // ===== SHARD-LEVEL REDUCE: Merge HLL sketches, convert to counts =====
            return reduceShardLevel(aggregations);
        } else {
            // ===== COORDINATOR REDUCE: Sum counts =====
            return reduceCoordinatorLevel(aggregations);
        }
    }

    /**
     * Shard-level reduce: Merges HLL sketches and converts to coordinator-level counts.
     *
     * <p>This method merges HLL sketches from Head and ClosedChunkIndex to deduplicate
     * time series within a shard, then converts the sketches to cardinality counts and
     * returns {@link CoordinatorLevelStats} to minimize network bandwidth.</p>
     */
    private InternalTSDBStats reduceShardLevel(List<InternalAggregation> aggregations) {
        HyperLogLogPlusPlus mergedSeriesSketch = null;
        Map<String, ShardLevelLabelStatsBuilder> builders = new HashMap<>();

        for (InternalAggregation agg : aggregations) {
            InternalTSDBStats stats = (InternalTSDBStats) agg;

            // All inputs must be shard-level stats
            if (stats.shardStats == null) {
                throw new IllegalStateException("Expected shard-level stats but got coordinator-level stats in shard reduce");
            }

            // Merge series cardinality sketches
            HyperLogLogPlusPlus sketch = stats.shardStats.getSeriesCardinalitySketch();
            if (sketch != null) {
                if (mergedSeriesSketch == null) {
                    mergedSeriesSketch = (HyperLogLogPlusPlus) sketch.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
                } else {
                    mergedSeriesSketch.merge(0, sketch, 0);
                }
            }

            // Merge per-label sketches
            for (Map.Entry<String, Map<String, HyperLogLogPlusPlus>> entry : stats.shardStats.getLabelStats().entrySet()) {
                String labelName = entry.getKey();
                Map<String, HyperLogLogPlusPlus> valueSketches = entry.getValue();

                ShardLevelLabelStatsBuilder builder = builders.computeIfAbsent(labelName, k -> new ShardLevelLabelStatsBuilder());

                // Merge value cardinality sketches
                if (valueSketches != null) {
                    if (builder.valueSketches == null) {
                        builder.valueSketches = new LinkedHashMap<>();
                    }
                    for (Map.Entry<String, HyperLogLogPlusPlus> ve : valueSketches.entrySet()) {
                        String valueKey = ve.getKey();
                        HyperLogLogPlusPlus newSketch = ve.getValue();
                        HyperLogLogPlusPlus existing = builder.valueSketches.get(valueKey);

                        if (existing == null) {
                            // First time seeing this value - clone the sketch (or keep null if newSketch is null)
                            if (newSketch != null) {
                                builder.valueSketches.put(
                                    valueKey,
                                    (HyperLogLogPlusPlus) newSketch.clone(0, BigArrays.NON_RECYCLING_INSTANCE)
                                );
                            } else {
                                builder.valueSketches.put(valueKey, null);
                            }
                        } else if (existing != null && newSketch != null) {
                            // Both are non-null - merge them
                            existing.merge(0, newSketch, 0);
                        } else if (existing == null && newSketch != null) {
                            // Existing is null but new is not - replace with clone
                            builder.valueSketches.put(valueKey, (HyperLogLogPlusPlus) newSketch.clone(0, BigArrays.NON_RECYCLING_INSTANCE));
                        }
                        // If both are null, or existing is non-null and new is null, keep existing
                    }
                }
            }
        }

        // Convert sketches to counts
        Long totalSeries = mergedSeriesSketch != null ? mergedSeriesSketch.cardinality(0) : null;

        Map<String, CoordinatorLevelStats.LabelStats> finalLabelStats = new HashMap<>();
        for (Map.Entry<String, ShardLevelLabelStatsBuilder> entry : builders.entrySet()) {
            String labelName = entry.getKey();
            ShardLevelLabelStatsBuilder builder = entry.getValue();

            // Get cardinality for this label (union of all value sketches)
            Long labelCardinality = null;
            Map<String, Long> valueCounts = null;

            if (builder.valueSketches != null) {
                // Check if any sketch is non-null (i.e., includeValueStats was true)
                boolean hasNonNullSketches = builder.valueSketches.values().stream().anyMatch(s -> s != null);

                if (hasNonNullSketches) {
                    // includeValueStats was true - compute actual cardinalities
                    HyperLogLogPlusPlus labelSketch = null;
                    valueCounts = new LinkedHashMap<>();

                    for (Map.Entry<String, HyperLogLogPlusPlus> ve : builder.valueSketches.entrySet()) {
                        HyperLogLogPlusPlus sketch = ve.getValue();

                        if (sketch != null) {
                            long valueCard = sketch.cardinality(0);
                            valueCounts.put(ve.getKey(), valueCard);

                            // Merge into label sketch for total label cardinality
                            if (labelSketch == null) {
                                labelSketch = (HyperLogLogPlusPlus) sketch.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
                            } else {
                                labelSketch.merge(0, sketch, 0);
                            }
                        }
                    }

                    labelCardinality = labelSketch != null ? labelSketch.cardinality(0) : null;
                } else {
                    // includeValueStats was false - extract value names from keys
                    // Keep valueCounts as null, but populate values list
                }
            }

            // Extract values list from builder (always populated, even when includeValueStats=false)
            List<String> valuesList = builder.valueSketches != null ? new ArrayList<>(builder.valueSketches.keySet()) : List.of();

            finalLabelStats.put(labelName, new CoordinatorLevelStats.LabelStats(labelCardinality, valuesList, valueCounts));
        }

        // Return coordinator-level stats (sketches converted to counts to save network bandwidth)
        CoordinatorLevelStats coordinatorStats = new CoordinatorLevelStats(totalSeries, finalLabelStats);
        return forCoordinatorLevel(name, null, coordinatorStats, metadata);
    }

    /**
     * Coordinator-level reduce: Sums counts from different shards.
     *
     * <p>This method sums pre-computed counts from different shards. No deduplication
     * is needed because each time series exists on only one shard (guaranteed by routing).</p>
     */
    private InternalTSDBStats reduceCoordinatorLevel(List<InternalAggregation> aggregations) {
        HeadStats mergedHeadStats = null; // TODO: Merge HeadStats in future when populated
        Long totalSeries = null;
        Map<String, LabelStatsBuilder> builders = new HashMap<>();

        for (InternalAggregation agg : aggregations) {
            InternalTSDBStats stats = (InternalTSDBStats) agg;

            // All inputs must be coordinator-level stats
            if (stats.coordinatorStats == null) {
                throw new IllegalStateException("Expected coordinator-level stats but got shard-level stats in coordinator reduce");
            }

            // Sum numSeries (already deduplicated at shard level)
            Long numSeries = stats.coordinatorStats.getTotalNumSeries();
            if (numSeries != null) {
                totalSeries = (totalSeries == null ? 0 : totalSeries) + numSeries;
            }

            // Merge label stats
            for (Map.Entry<String, CoordinatorLevelStats.LabelStats> entry : stats.coordinatorStats.getLabelStats().entrySet()) {
                String labelName = entry.getKey();
                CoordinatorLevelStats.LabelStats ls = entry.getValue();

                LabelStatsBuilder builder = builders.computeIfAbsent(labelName, k -> new LabelStatsBuilder());

                // Collect values (always present)
                builder.values.addAll(ls.getValues());

                // Sum numSeries per label if present
                if (ls.getNumSeries() != null) {
                    builder.hasNumSeries = true;
                    builder.numSeries = (builder.numSeries == null ? 0 : builder.numSeries) + ls.getNumSeries();
                }

                // Sum value counts
                if (ls.getValuesStats() != null) {
                    if (builder.valueCounts == null) {
                        builder.valueCounts = new LinkedHashMap<>();
                    }
                    for (Map.Entry<String, Long> ve : ls.getValuesStats().entrySet()) {
                        builder.valueCounts.merge(ve.getKey(), ve.getValue(), Long::sum);
                    }
                }
            }
        }

        // Build final result
        Map<String, CoordinatorLevelStats.LabelStats> finalLabelStats = new HashMap<>();
        for (Map.Entry<String, LabelStatsBuilder> entry : builders.entrySet()) {
            LabelStatsBuilder builder = entry.getValue();
            finalLabelStats.put(
                entry.getKey(),
                new CoordinatorLevelStats.LabelStats(
                    builder.hasNumSeries ? builder.numSeries : null,
                    new ArrayList<>(builder.values),
                    builder.valueCounts
                )
            );
        }

        CoordinatorLevelStats coordinatorStats = new CoordinatorLevelStats(totalSeries, finalLabelStats);
        return forCoordinatorLevel(name, mergedHeadStats, coordinatorStats, metadata);
    }

    /**
     * Helper class for building shard-level label stats during reduce.
     */
    private static class ShardLevelLabelStatsBuilder {
        Map<String, HyperLogLogPlusPlus> valueSketches = null;
    }

    /**
     * Helper class for building coordinator-level label stats during reduce.
     */
    private static class LabelStatsBuilder {
        boolean hasNumSeries = false;
        Long numSeries = null;
        Set<String> values = new LinkedHashSet<>(); // Preserve insertion order
        Map<String, Long> valueCounts = null;
    }

    /**
     * Retrieves a property value based on the given path.
     *
     * @param path the property path to retrieve
     * @return the property value
     * @throws IllegalArgumentException if the property path is unknown
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String property = path.get(0);
            if ("numSeries".equals(property)) {
                return getNumSeries();
            } else if ("labelStats".equals(property)) {
                return getLabelStats();
            }
        }
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for TSDBStatsAggregation [" + name + "]");
    }

    /**
     * Returns the head statistics.
     *
     * @return the head statistics, or null if not requested
     */
    public HeadStats getHeadStats() {
        return headStats;
    }

    /**
     * Returns the total number of unique time series.
     *
     * <p>Only available for coordinator-level stats. Returns null for shard-level stats.</p>
     *
     * @return the total time series count, or null if not available
     */
    public Long getNumSeries() {
        return coordinatorStats != null ? coordinatorStats.getTotalNumSeries() : null;
    }

    /**
     * Returns the label statistics map.
     *
     * <p>Only available for coordinator-level stats. Returns empty map for shard-level stats.</p>
     *
     * @return the label statistics map
     */
    public Map<String, CoordinatorLevelStats.LabelStats> getLabelStats() {
        return coordinatorStats != null ? coordinatorStats.getLabelStats() : Map.of();
    }

    /**
     * Serializes the TSDB statistics to XContent format (grouped format).
     *
     * <p>Only coordinator-level stats are serialized to XContent for the final response.
     * Shard-level stats are not exposed externally.</p>
     *
     * @param builder the XContent builder to write to
     * @param params the serialization parameters
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // Write headStats if present
        if (headStats != null) {
            builder.startObject("headStats");
            builder.field("numSeries", headStats.numSeries());
            builder.field("chunkCount", headStats.chunkCount());
            builder.field("minTime", headStats.minTime());
            builder.field("maxTime", headStats.maxTime());
            builder.endObject();
        }

        builder.startObject("labelStats");

        // Only serialize coordinator-level stats (skip shard-level stats)
        if (coordinatorStats != null) {
            // Write numSeries at the start of labelStats
            Long totalNumSeries = coordinatorStats.getTotalNumSeries();
            if (totalNumSeries != null) {
                builder.field("numSeries", totalNumSeries);
            }

            for (Map.Entry<String, CoordinatorLevelStats.LabelStats> entry : coordinatorStats.getLabelStats().entrySet()) {
                builder.startObject(entry.getKey());
                CoordinatorLevelStats.LabelStats stats = entry.getValue();
                if (stats.getNumSeries() != null) {
                    builder.field("numSeries", stats.getNumSeries());
                }
                builder.field("values", stats.getValues());
                if (stats.getValuesStats() != null) {
                    builder.field("valuesStats", stats.getValuesStats());
                }
                builder.endObject();
            }
        }

        builder.endObject();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there's only
     * a single internal aggregation.
     *
     * @return false, as InternalTSDBStats does not require reduction for single aggregations
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTSDBStats that = (InternalTSDBStats) o;
        return Objects.equals(headStats, that.headStats)
            && Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && Objects.equals(shardStats, that.shardStats)
            && Objects.equals(coordinatorStats, that.coordinatorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getMetadata(), headStats, shardStats, coordinatorStats);
    }
}
