/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

/** Metric names, descriptions, and units. */
public final class TSDBMetricsConstants {

    private TSDBMetricsConstants() {
        // Utility class, no instantiation
    }

    // ============================================
    // Ingestion Metrics (Engine-level)
    // ============================================

    /** Counter: Total number of samples ingested into TSDB across all shards */
    public static final String SAMPLES_INGESTED_TOTAL = "tsdb.samples.ingested.total";

    /** Counter: Total number of time series created across all shards */
    public static final String SERIES_CREATED_TOTAL = "tsdb.series.created.total";

    /** Counter: Total number of memory chunks created across all shards */
    public static final String MEMCHUNKS_CREATED_TOTAL = "tsdb.memchunks.created.total";

    /** Counter: Total number of out-of-order samples rejected across all shards */
    public static final String OOO_SAMPLES_REJECTED_TOTAL = "tsdb.ooo_samples.rejected.total";

    /** Counter: Total number of out-of-order chunks created across all shards */
    public static final String OOO_CHUNKS_CREATED_TOTAL = "tsdb.ooo_chunks.created.total";

    /** Counter: Total number of out-of-order chunks merged across all shards */
    public static final String OOO_CHUNKS_MERGED_TOTAL = "tsdb.ooo_chunks.merged.total";

    // ============================================
    // Aggregation Metrics (TimeSeriesUnfoldAggregator)
    // ============================================

    /** Histogram: Latency of collect() operation per request */
    public static final String AGGREGATION_COLLECT_LATENCY = "tsdb.aggregation.collect.latency";

    /** Histogram: Latency of postCollect() operation per request */
    public static final String AGGREGATION_POST_COLLECT_LATENCY = "tsdb.aggregation.post_collect.latency";

    /** Histogram: Total Lucene documents processed per request */
    public static final String AGGREGATION_DOCS_TOTAL = "tsdb.aggregation.docs.total";

    /** Histogram: Live index documents processed per request */
    public static final String AGGREGATION_DOCS_LIVE = "tsdb.aggregation.docs.live";

    /** Histogram: Closed chunk index documents processed per request */
    public static final String AGGREGATION_DOCS_CLOSED = "tsdb.aggregation.docs.closed";

    /** Histogram: Total chunks processed per request */
    public static final String AGGREGATION_CHUNKS_TOTAL = "tsdb.aggregation.chunks.total";

    /** Histogram: Live chunks processed per request */
    public static final String AGGREGATION_CHUNKS_LIVE = "tsdb.aggregation.chunks.live";

    /** Histogram: Closed chunks processed per request */
    public static final String AGGREGATION_CHUNKS_CLOSED = "tsdb.aggregation.chunks.closed";

    /** Histogram: Total samples processed per request */
    public static final String AGGREGATION_SAMPLES_TOTAL = "tsdb.aggregation.samples.total";

    /** Histogram: Live samples processed per request */
    public static final String AGGREGATION_SAMPLES_LIVE = "tsdb.aggregation.samples.live";

    /** Histogram: Closed samples processed per request */
    public static final String AGGREGATION_SAMPLES_CLOSED = "tsdb.aggregation.samples.closed";

    /** Counter: Total chunksForDoc() errors */
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL = "tsdb.aggregation.chunks_for_doc.errors.total";

    // ============================================
    // Metric Descriptions
    // ============================================

    // Ingestion Metrics
    public static final String SAMPLES_INGESTED_TOTAL_DESC = "Total number of samples ingested into TSDB across all shards";
    public static final String SERIES_CREATED_TOTAL_DESC = "Total number of time series created across all shards";
    public static final String MEMCHUNKS_CREATED_TOTAL_DESC = "Total number of memory chunks created across all shards";
    public static final String OOO_SAMPLES_REJECTED_TOTAL_DESC = "Total number of out-of-order samples rejected across all shards";
    public static final String OOO_CHUNKS_CREATED_TOTAL_DESC = "Total number of out-of-order chunks created across all shards";
    public static final String OOO_CHUNKS_MERGED_TOTAL_DESC = "Total number of out-of-order chunks merged across all shards";

    // Aggregation Metrics
    public static final String AGGREGATION_COLLECT_LATENCY_DESC = "Latency of collect() operation per aggregation request";
    public static final String AGGREGATION_POST_COLLECT_LATENCY_DESC = "Latency of postCollect() operation per aggregation request";
    public static final String AGGREGATION_DOCS_TOTAL_DESC = "Total Lucene documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_LIVE_DESC = "Live index documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_CLOSED_DESC = "Closed chunk index documents processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_TOTAL_DESC = "Total chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_LIVE_DESC = "Live chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_CLOSED_DESC = "Closed chunks processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_TOTAL_DESC = "Total samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_LIVE_DESC = "Live samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_CLOSED_DESC = "Closed samples processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC = "Total errors in chunksForDoc() operations";

    // ============================================
    // Metric Units
    // ============================================

    /** Unit for dimensionless counts */
    public static final String UNIT_COUNT = "1";

    /** Unit for milliseconds */
    public static final String UNIT_MILLISECONDS = "ms";

    /** Unit for bytes */
    public static final String UNIT_BYTES = "bytes";
}
