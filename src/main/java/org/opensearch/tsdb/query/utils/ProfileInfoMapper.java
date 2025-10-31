/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProfileInfoMapper {

    public static final String PROFILE_FIELD_NAME = "profile";
    public static final String M3QL_FIELD_NAME = "m3ql";
    public static final String TOTAL_CHUNKS = "total_chunks";
    public static final String TOTAL_SAMPLES = "total_samples";
    public static final String TOTAL_INPUT_SERIES = "total_input_series";
    public static final String TOTAL_OUTPUT_SERIES = "total_output_series";
    public static final String LIVE_CHUNK_COUNT = "live_chunk_count";
    public static final String CLOSED_CHUNK_COUNT = "closed_chunk_count";
    public static final String LIVE_DOC_COUNT = "live_doc_count";
    public static final String CLOSED_DOC_COUNT = "closed_doc_count";
    public static final String LIVE_SAMPLE_COUNT = "live_sample_count";
    public static final String CLOSED_SAMPLE_COUNT = "closed_sample_count";
    public static final String STAGES_FIELD_NAME = "stages";
    public static final String TOTALS_FIELD_NAME = "totals";
    public static final String DEFAULT = "default";

    /**
     * Extract Profile Info from TimeSeriesUnfoldAggregator and construct debug info for every stage
     * @param response
     * @param builder
     * @throws IOException
     */
    public static void extractProfileInfo(SearchResponse response, XContentBuilder builder) throws IOException {
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        if (profileResults != null && !profileResults.isEmpty()) {
            // Aggregate debug info from all shards
            // Extract DebugInfo from Profile Info
            DebugStats debugStats = new DebugStats();

            for (ProfileShardResult shardResult : profileResults.values()) {
                if (shardResult.getAggregationProfileResults() != null) {
                    for (ProfileResult profileResult : shardResult.getAggregationProfileResults().getProfileResults()) {
                        extractDebugInfoFromProfileResult(profileResult, debugStats);
                    }
                }
            }
            builder.startObject(PROFILE_FIELD_NAME);
            writeDebugInfoToXContent(builder, debugStats);
            // TODO add profiling metrics
            builder.endObject();

        }
    }

    private static void writeDebugInfoToXContent(XContentBuilder builder, DebugStats debugStats) throws IOException {
        // Only add debug section if we found any debug data
        if (!debugStats.stageStatsMap.isEmpty()) {
            builder.startObject(M3QL_FIELD_NAME);

            // Calculate and output totals across all stages
            StageStats totals = calculateTotals(debugStats.stageStatsMap);
            builder.startObject(TOTALS_FIELD_NAME);
            writeStageStats(builder, totals);
            builder.endObject();

            // Stage-specific breakdown
            builder.startObject(STAGES_FIELD_NAME);
            for (Map.Entry<String, StageStats> entry : debugStats.stageStatsMap.entrySet()) {
                String stageName = entry.getKey();
                StageStats stageStats = entry.getValue();

                builder.startObject(stageName);
                writeStageStats(builder, stageStats);
                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }
    }

    private static void extractDebugInfoFromProfileResult(ProfileResult profileResult, DebugStats stats) {
        // Check if this is a time_series_unfold aggregation
        if (TimeSeriesUnfoldAggregator.class.getName().equals(profileResult.getQueryName())) {
            Map<String, Object> debugMap = profileResult.getDebugInfo();
            if (debugMap != null && !debugMap.isEmpty()) {
                // Extract stage name (could be comma-separated list)
                String stages = (String) debugMap.get(STAGES_FIELD_NAME);
                // safety check for case that stages in TimeSeriesUnfoldAggregator is null
                if (stages == null || stages.isEmpty()) {
                    stages = DEFAULT; // fallback for aggregations without explicit stages
                }

                // Create stage-specific stats
                StageStats stageStats = stats.stageStatsMap.computeIfAbsent(stages, k -> new StageStats());

                // Accumulate stats for this stage
                stageStats.chunkCount += getLongValue(debugMap, TOTAL_CHUNKS);
                stageStats.sampleCount += getLongValue(debugMap, TOTAL_SAMPLES);
                stageStats.liveChunksCount += getLongValue(debugMap, LIVE_CHUNK_COUNT);
                stageStats.closedChunksCount += getLongValue(debugMap, CLOSED_CHUNK_COUNT);
                stageStats.liveDocCount += getLongValue(debugMap, LIVE_DOC_COUNT);
                stageStats.closedDocCount += getLongValue(debugMap, CLOSED_DOC_COUNT);
                stageStats.liveSampleCount += getLongValue(debugMap, LIVE_SAMPLE_COUNT);
                stageStats.closedSampleCount += getLongValue(debugMap, CLOSED_SAMPLE_COUNT);
                stageStats.inputSeriesCount += getLongValue(debugMap, TOTAL_INPUT_SERIES);
                stageStats.outputSeriesCount += getLongValue(debugMap, TOTAL_OUTPUT_SERIES);
            }
        }
    }

    /**
     * Calculates total metrics across all stages.
     */
    private static StageStats calculateTotals(Map<String, StageStats> stageStatsMap) {
        StageStats totals = new StageStats();

        for (StageStats stats : stageStatsMap.values()) {
            totals.chunkCount += stats.chunkCount;
            totals.sampleCount += stats.sampleCount;
            totals.inputSeriesCount += stats.inputSeriesCount;
            totals.outputSeriesCount += stats.outputSeriesCount;
            totals.liveChunksCount += stats.liveChunksCount;
            totals.closedChunksCount += stats.closedChunksCount;
            totals.liveDocCount += stats.liveDocCount;
            totals.closedDocCount += stats.closedDocCount;
            totals.liveSampleCount += stats.liveSampleCount;
            totals.closedSampleCount += stats.closedSampleCount;
        }

        return totals;
    }

    /**
     * Writes stage statistics to the XContentBuilder.
     */
    private static void writeStageStats(XContentBuilder builder, StageStats stats) throws IOException {
        builder.field(TOTAL_CHUNKS, stats.chunkCount);
        builder.field(TOTAL_SAMPLES, stats.sampleCount);
        builder.field(TOTAL_INPUT_SERIES, stats.inputSeriesCount);
        builder.field(TOTAL_OUTPUT_SERIES, stats.outputSeriesCount);
        builder.field(LIVE_CHUNK_COUNT, stats.liveChunksCount);
        builder.field(CLOSED_CHUNK_COUNT, stats.closedChunksCount);
        builder.field(LIVE_DOC_COUNT, stats.liveDocCount);
        builder.field(CLOSED_DOC_COUNT, stats.closedDocCount);
        builder.field(LIVE_SAMPLE_COUNT, stats.liveSampleCount);
        builder.field(CLOSED_SAMPLE_COUNT, stats.closedSampleCount);
    }

    private static long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    private static class DebugStats {
        Map<String, StageStats> stageStatsMap = new HashMap<>();
    }

    private static class StageStats {
        long chunkCount = 0;
        long sampleCount = 0;
        long inputSeriesCount = 0;
        long outputSeriesCount = 0;
        long liveChunksCount = 0;
        long closedChunksCount = 0;
        long liveDocCount = 0;
        long closedDocCount = 0;
        long liveSampleCount = 0;
        long closedSampleCount = 0;
    }

}
