/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Aggregator that collects TSDB statistics from time series data.
 *
 * <p>This aggregator processes time series documents and builds statistics about
 * label keys and their values, using exact fingerprint counting for precise results.</p>
 *
 * <h2>Features:</h2>
 * <ul>
 *   <li><strong>Label Cardinality:</strong> Tracks unique label values per key using exact fingerprints</li>
 *   <li><strong>Time Series Counting:</strong> Counts unique time series exactly using fingerprints</li>
 *   <li><strong>Value Statistics:</strong> Per-value exact cardinality counts</li>
 *   <li><strong>Exact Counting:</strong> No approximation errors</li>
 * </ul>
 *
 * @since 0.0.1
 */
public class TSDBStatsAggregator extends MetricsAggregator {

    private final long minTimestamp;
    private final long maxTimestamp;
    private final boolean includeValueStats;

    // Fingerprint set for tracking unique time series
    private final Set<Long> uniqueSeriesFingerprints;

    // BytesRefHash to store "key:value" strings and assign ordinals (shard-level, shared across segments)
    private final BytesRefHash labelValuePairOrdinalMap;

    // Track fingerprint set per ordinal: ordinal -> Set<Long>
    // Key grouping is deferred to buildAggregation() to avoid parsing in hot path
    private final Map<Integer, Set<Long>> ordinalFingerprintSets;

    /**
     * Creates a TSDB stats aggregator.
     *
     * @param name The name of the aggregator
     * @param context The search context
     * @param parent The parent aggregator
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param includeValueStats Whether to include per-value statistics
     * @param metadata The aggregation metadata
     * @throws IOException If an error occurs during initialization
     */
    public TSDBStatsAggregator(
        String name,
        SearchContext context,
        Aggregator parent,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.includeValueStats = includeValueStats;

        // Initialize fingerprint set for exact series tracking
        this.uniqueSeriesFingerprints = new HashSet<>();

        // Initialize BytesRefHash for ordinals
        ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
        this.labelValuePairOrdinalMap = new BytesRefHash(pool);

        this.ordinalFingerprintSets = includeValueStats ? new HashMap<>() : null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Check if this leaf reader can be pruned based on time range
        TSDBLeafReader tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(ctx.reader());
        if (tsdbLeafReader == null) {
            throw new IOException("Expected TSDBLeafReader but found: " + ctx.reader().getClass().getName());
        }
        if (!tsdbLeafReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            // No matching data in this segment, skip it
            return sub;
        }

        return new TSDBStatsLeafBucketCollector(ctx, tsdbLeafReader, sub);
    }

    private class TSDBStatsLeafBucketCollector extends LeafBucketCollectorBase {

        private final TSDBLeafReader tsdbLeafReader;
        private TSDBDocValues tsdbDocValues;

        public TSDBStatsLeafBucketCollector(LeafReaderContext ctx, TSDBLeafReader tsdbLeafReader, LeafBucketCollector sub)
            throws IOException {
            super(sub, null);
            this.tsdbLeafReader = tsdbLeafReader;
            this.tsdbDocValues = this.tsdbLeafReader.getTSDBDocValues();
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            // Get labels for this document
            Labels labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);

            // Use labels hashCode as fingerprint for exact deduplication across chunks/indexes
            long fingerprint = labels.hashCode();

            // Add to total series fingerprint set
            uniqueSeriesFingerprints.add(fingerprint);

            // Process each label using BytesRef directly (avoid String conversion)
            BytesRef[] keyValuePairs = labels.toKeyValueBytesRefs();
            for (BytesRef keyValue : keyValuePairs) {
                // Add "key:value" to hash and get ordinal
                long ord = labelValuePairOrdinalMap.add(keyValue);
                if (ord < 0) {
                    // Already exists, get existing ordinal
                    ord = -1 - ord;
                }
                int ordinal = (int) ord;

                // Track fingerprints per value if enabled
                if (ordinalFingerprintSets != null) {
                    Set<Long> fingerprintSet = ordinalFingerprintSets.computeIfAbsent(ordinal, k -> new HashSet<>());
                    fingerprintSet.add(fingerprint);
                }
            }
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        // Build map of label name -> Map of label value -> fingerprint Set
        Map<String, Map<String, Set<Long>>> shardLabelStats = new HashMap<>();

        // Reusable BytesRef for lookups
        BytesRef scratch = new BytesRef();

        // Iterate all ordinals in labelValuePairOrdinalMap and group by key
        for (int ordinal = 0; ordinal < labelValuePairOrdinalMap.size(); ordinal++) {
            // Lookup "key:value" from ordinal
            labelValuePairOrdinalMap.get(ordinal, scratch);
            String keyValue = scratch.utf8ToString(); // "service:api"

            // Parse to extract key and value
            int colonIndex = keyValue.indexOf(':');
            if (colonIndex < 0) {
                throw new IllegalStateException("Invalid label format - missing colon: " + keyValue);
            }
            String key = keyValue.substring(0, colonIndex); // "service"
            String value = keyValue.substring(colonIndex + 1); // "api"

            // Get or create value fingerprint sets map for this key (always create to track which values exist)
            Map<String, Set<Long>> valueFingerprintSets = shardLabelStats.computeIfAbsent(key, k -> new LinkedHashMap<>());

            // Add fingerprint set for this value if includeValueStats is enabled, otherwise add null as marker
            if (includeValueStats && ordinalFingerprintSets != null) {
                Set<Long> fingerprintSet = ordinalFingerprintSets.get(ordinal);
                if (fingerprintSet != null) {
                    valueFingerprintSets.put(value, fingerprintSet);
                }
            } else {
                // Track that this value exists, but use null set (will be converted to count=0 in reduce)
                valueFingerprintSets.put(value, null);
            }
        }

        // Create shard-level stats with fingerprint sets
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(
            uniqueSeriesFingerprints,
            shardLabelStats,
            includeValueStats  // Pass global flag so coordinator knows if value stats were collected
        );

        // Return using factory method
        return InternalTSDBStats.forShardLevel(name, shardStats, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        // Return empty coordinator-level stats
        InternalTSDBStats.CoordinatorLevelStats emptyStats = new InternalTSDBStats.CoordinatorLevelStats(null, Map.of());
        return InternalTSDBStats.forCoordinatorLevel(name, null, emptyStats, metadata());
    }

    @Override
    public void doClose() {
        // Clear fingerprint sets
        if (ordinalFingerprintSets != null) {
            ordinalFingerprintSets.clear();
        }
        if (uniqueSeriesFingerprints != null) {
            uniqueSeriesFingerprints.clear();
        }
        // Close BytesRefHash
        if (labelValuePairOrdinalMap != null) {
            try {
                labelValuePairOrdinalMap.close();
            } catch (Exception e) {
                // Log and continue - BytesRefHash may fail on multiple close calls
            }
        }
    }
}
