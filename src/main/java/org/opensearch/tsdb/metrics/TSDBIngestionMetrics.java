/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;

/**
 * Ingestion-related TSDB metrics (engine-level counters).
 */
public class TSDBIngestionMetrics {
    /** Counter for total number of samples ingested into TSDB */
    public Counter samplesIngested;

    /** Counter for total number of time series created */
    public Counter seriesCreated;

    /** Counter for total number of memory chunks created */
    public Counter memChunksCreated;

    /** Counter for total number of out-of-order samples rejected */
    public Counter oooSamplesRejected;

    /** Counter for OOO chunks created */
    public Counter oooChunksCreated;

    /** Counter for OOO chunks merged */
    public Counter oooChunksMerged;

    /**
     * Initialize ingestion metrics. Called by TSDBMetrics.initialize().
     */
    public void initialize(MetricsRegistry registry) {
        samplesIngested = registry.createCounter(
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL,
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        seriesCreated = registry.createCounter(
            TSDBMetricsConstants.SERIES_CREATED_TOTAL,
            TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        memChunksCreated = registry.createCounter(
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooSamplesRejected = registry.createCounter(
            TSDBMetricsConstants.OOO_SAMPLES_REJECTED_TOTAL,
            TSDBMetricsConstants.OOO_SAMPLES_REJECTED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooChunksCreated = registry.createCounter(
            TSDBMetricsConstants.OOO_CHUNKS_CREATED_TOTAL,
            TSDBMetricsConstants.OOO_CHUNKS_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooChunksMerged = registry.createCounter(
            TSDBMetricsConstants.OOO_CHUNKS_MERGED_TOTAL,
            TSDBMetricsConstants.OOO_CHUNKS_MERGED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    /**
     * Cleanup ingestion metrics (for tests).
     */
    public void cleanup() {
        samplesIngested = null;
        seriesCreated = null;
        memChunksCreated = null;
        oooSamplesRejected = null;
        oooChunksCreated = null;
        oooChunksMerged = null;
    }
}
