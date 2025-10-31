/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ProfileInfoMapper.
 */
public class ProfileInfoMapperTests extends OpenSearchTestCase {

    public void testExtractProfileInfo_WithNullProfileResults() throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getProfileResults()).thenReturn(null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertFalse("Should not contain profile field when profile results are null", json.contains("\"profile\""));
    }

    public void testExtractProfileInfo_WithEmptyProfileResults() throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getProfileResults()).thenReturn(Collections.emptyMap());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertFalse("Should not contain profile field when profile results are empty", json.contains("\"profile\""));
    }

    public void testExtractProfileInfo_WithValidDebugInfo() throws IOException {
        // Arrange
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES, 500L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 10L);
        debugMap.put(ProfileInfoMapper.LIVE_CHUNK_COUNT, 50L);
        debugMap.put(ProfileInfoMapper.CLOSED_CHUNK_COUNT, 50L);
        debugMap.put(ProfileInfoMapper.LIVE_DOC_COUNT, 200L);
        debugMap.put(ProfileInfoMapper.CLOSED_DOC_COUNT, 300L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLE_COUNT, 250L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLE_COUNT, 250L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain profile field", json.contains("\"profile\""));
        assertTrue("Should contain m3ql field", json.contains("\"m3ql\""));
        assertTrue("Should contain stages field", json.contains("\"stages\""));
        assertTrue("Should contain totals field", json.contains("\"totals\""));
        assertTrue("Should contain ScaleStage", json.contains("\"ScaleStage\""));
    }

    public void testExtractProfileInfo_WithMultipleStages() throws IOException {
        // Arrange
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage,SumStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 150L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 5L);
        debugMap.put(ProfileInfoMapper.LIVE_CHUNK_COUNT, 75L);
        debugMap.put(ProfileInfoMapper.CLOSED_CHUNK_COUNT, 75L);
        debugMap.put(ProfileInfoMapper.LIVE_DOC_COUNT, 300L);
        debugMap.put(ProfileInfoMapper.CLOSED_DOC_COUNT, 450L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLE_COUNT, 375L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLE_COUNT, 375L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain ScaleStage,SumStage", json.contains("\"ScaleStage,SumStage\""));
    }

    public void testExtractProfileInfo_WithDefaultStage() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 50L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 5L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain default stage", json.contains("\"default\""));
    }

    public void testExtractProfileInfo_WithEmptyStagesField() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 50L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain default stage when stages field is empty", json.contains("\"default\""));
    }

    public void testExtractProfileInfo_WithMultipleShards() throws IOException {
        Map<String, Object> debugMap1 = new HashMap<>();
        debugMap1.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap1.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap1.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);
        debugMap1.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 10L);

        Map<String, Object> debugMap2 = new HashMap<>();
        debugMap2.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap2.put(ProfileInfoMapper.TOTAL_CHUNKS, 200L);
        debugMap2.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);
        debugMap2.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 20L);

        ProfileResult profileResult1 = createProfileResult(debugMap1);
        ProfileResult profileResult2 = createProfileResult(debugMap2);
        SearchResponse response = createSearchResponse(List.of(profileResult1, profileResult2));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should aggregate stats across shards", json.contains("\"total_chunks\":300"));
        assertTrue("Should aggregate input series", json.contains("\"total_input_series\":30"));
        assertTrue("Should aggregate output series", json.contains("\"total_output_series\":30"));
    }

    public void testExtractProfileInfo_WithNonTimeSeriesUnfoldAggregator() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);

        ProfileResult profileResult = createProfileResult("SomeOtherAggregator", debugMap);

        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertFalse("Should not extract debug info from non-TimeSeriesUnfoldAggregator", json.contains("\"m3ql\""));
    }

    public void testExtractProfileInfo_WithNullDebugMap() throws IOException {
        ProfileResult profileResult = createProfileResult(null);
        SearchResponse response = createSearchResponse(List.of(profileResult));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertFalse("Should not add debug section when debug map is null", json.contains("\"m3ql\""));
    }

    public void testExtractProfileInfo_WithNonNumericValues() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, "not_a_number"); // Non-numeric value
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should handle non-numeric values gracefully", json.contains("\"total_chunks\":0"));
        assertTrue("Should still process numeric values", json.contains("\"total_input_series\":10"));
    }

    public void testExtractProfileInfo_WithIntegerValues() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100); // Integer instead of Long
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should handle integer values", json.contains("\"total_chunks\":100"));
        assertTrue("Should handle integer values", json.contains("\"total_input_series\":10"));
    }

    public void testExtractProfileInfo_WithAllMetrics() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "CompleteStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 1000L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES, 5000L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 100L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 50L);
        debugMap.put(ProfileInfoMapper.LIVE_CHUNK_COUNT, 500L);
        debugMap.put(ProfileInfoMapper.CLOSED_CHUNK_COUNT, 500L);
        debugMap.put(ProfileInfoMapper.LIVE_DOC_COUNT, 2000L);
        debugMap.put(ProfileInfoMapper.CLOSED_DOC_COUNT, 3000L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLE_COUNT, 2500L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLE_COUNT, 2500L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain total_chunks", json.contains("\"total_chunks\":1000"));
        assertTrue("Should contain total_samples", json.contains("\"total_samples\":5000"));
        assertTrue("Should contain total_input_series", json.contains("\"total_input_series\":100"));
        assertTrue("Should contain total_output_series", json.contains("\"total_output_series\":50"));
        assertTrue("Should contain live_chunk_count", json.contains("\"live_chunk_count\":500"));
        assertTrue("Should contain closed_chunk_count", json.contains("\"closed_chunk_count\":500"));
        assertTrue("Should contain live_doc_count", json.contains("\"live_doc_count\":2000"));
        assertTrue("Should contain closed_doc_count", json.contains("\"closed_doc_count\":3000"));
        assertTrue("Should contain live_sample_count", json.contains("\"live_sample_count\":2500"));
        assertTrue("Should contain closed_sample_count", json.contains("\"closed_sample_count\":2500"));
    }

    public void testExtractProfileInfo_WithMultipleDifferentStages() throws IOException {
        Map<String, Object> debugMap1 = new HashMap<>();
        debugMap1.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap1.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap1.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);

        Map<String, Object> debugMap2 = new HashMap<>();
        debugMap2.put(ProfileInfoMapper.STAGES_FIELD_NAME, "SumStage");
        debugMap2.put(ProfileInfoMapper.TOTAL_CHUNKS, 200L);
        debugMap2.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);

        ProfileResult profileResult1 = createProfileResult(debugMap1);
        ProfileResult profileResult2 = createProfileResult(debugMap2);
        SearchResponse response = createSearchResponse(List.of(profileResult1, profileResult2));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain ScaleStage", json.contains("\"ScaleStage\""));
        assertTrue("Should contain SumStage", json.contains("\"SumStage\""));
        // Totals should aggregate both stages
        assertTrue("Totals should aggregate both stages", json.contains("\"total_chunks\":300"));
        assertTrue("Totals should aggregate both stages", json.contains("\"total_input_series\":30"));
    }

    public void testExtractProfileInfo_WithZeroValues() throws IOException {
        // Arrange
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "EmptyStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 0L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 0L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 0L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should handle zero values", json.contains("\"total_chunks\":0"));
        assertTrue("Should handle zero values", json.contains("\"total_input_series\":0"));
    }

    public void testExtractProfileInfo_WithMissingFields() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "PartialStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        // Missing other fields

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertTrue("Should contain provided field", json.contains("\"total_chunks\":100"));
        // Missing fields should default to 0
        assertTrue("Missing fields should default to 0", json.contains("\"total_input_series\":0"));
        assertTrue("Missing fields should default to 0", json.contains("\"total_output_series\":0"));
    }

    // ============================
    // Helper Methods
    // ============================

    private ProfileResult createProfileResult(Map<String, Object> debugMap) {
        return createProfileResult(TimeSeriesUnfoldAggregator.class.getName(), debugMap);

    }

    private ProfileResult createProfileResult(String type, Map<String, Object> debugMap) {
        return new ProfileResult(type, "testCase", Collections.emptyMap(), debugMap, 1000l, Collections.emptyList());
    }

    private SearchResponse createSearchResponse(List<ProfileResult> profileResults) {
        SearchResponse response = mock(SearchResponse.class);
        Map<String, ProfileShardResult> profileShardResults = new HashMap<>();

        AggregationProfileShardResult aggProfileShardResult = new AggregationProfileShardResult(new ArrayList<>(profileResults));

        ProfileShardResult shardResult = mock(ProfileShardResult.class);
        when(shardResult.getAggregationProfileResults()).thenReturn(aggProfileShardResult);

        profileShardResults.put("shard_0", shardResult);
        when(response.getProfileResults()).thenReturn(profileShardResults);

        return response;
    }
}
