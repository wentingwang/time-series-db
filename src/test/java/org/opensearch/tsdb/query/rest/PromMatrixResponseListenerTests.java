/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.opensearch.tsdb.query.rest.RestTestUtils.getResultArray;
import static org.opensearch.tsdb.query.rest.RestTestUtils.parseJsonResponse;
import static org.opensearch.tsdb.query.rest.RestTestUtils.validateErrorResponse;
import static org.opensearch.tsdb.query.rest.RestTestUtils.validateSuccessResponse;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponse;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponseWithMultipleAggregations;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponseWithNullAggregations;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithAlias;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithEmptySamples;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithLabels;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithNullAlias;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithNullLabels;

/**
 * Comprehensive test suite for PromMatrixResponseListener.
 *
 * <p>This test class provides complete coverage of all code paths including:
 * <ul>
 *   <li>Successful transformation scenarios with various data types</li>
 *   <li>Error handling scenarios</li>
 *   <li>Edge cases like null/empty data</li>
 *   <li>Response format validation</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class PromMatrixResponseListenerTests extends OpenSearchTestCase {

    // ========== Test Data Constants ==========

    private static final String TEST_AGG_NAME = "test_aggregation";
    private static final Map<String, Object> TEST_METADATA = Collections.emptyMap();

    // ========== Constructor Tests ==========

    public void testConstructor() {
        // Arrange & Act
        PromMatrixResponseListener listenerWithName = new PromMatrixResponseListener(
            new FakeRestChannel(new FakeRestRequest(), true, 1),
            TEST_AGG_NAME,
            false,
            false
        );

        // Assert
        assertNotNull(listenerWithName);
    }

    public void testConstructorRejectsNull() {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);

        // Act & Assert
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> new PromMatrixResponseListener(channel, null, false, false)
        );
        assertEquals("finalAggregationName cannot be null", exception.getMessage());
    }

    // ========== Successful Response Tests ==========

    public void testBuildResponseWithLabels() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"metric\""));
        assertThat(responseContent, containsString("\"region\""));
        assertThat(responseContent, containsString("\"us-east\""));
        assertThat(responseContent, containsString("\"service\""));
        assertThat(responseContent, containsString("\"api\""));
    }

    public void testBuildResponseWithAlias() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithAlias();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"__name__\""));
        assertThat(responseContent, containsString("\"my_metric\""));
    }

    // ========== Filter by Aggregation Name Tests ==========

    public void testBuildResponseWithMatchingAggregationName() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        // Filter for only "agg2" - should match only that aggregation
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, "agg2", false, false);

        // Create response with 3 different aggregations (agg1, agg2, agg3)
        SearchResponse searchResponse = createSearchResponseWithMultipleAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Should include ONLY the matching aggregation (agg2)
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);

        // Should have exactly 1 time series from agg2
        assertThat(results, hasSize(1));

        // Verify it's the correct one from agg2
        Map<String, String> metric = (Map<String, String>) results.get(0).get("metric");
        assertThat(metric.get("__name__"), equalTo("metric-from-agg2"));
    }

    public void testBuildResponseWithNonMatchingAggregationName() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        // Filter for "nonexistent_agg" - should not match any aggregation
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, "nonexistent_agg", false, false);

        // Create response with 3 different aggregations (agg1, agg2, agg3)
        SearchResponse searchResponse = createSearchResponseWithMultipleAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Should have NO results since name doesn't match any aggregation
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);

        // Should have 0 time series (no matches)
        assertThat(results, hasSize(0));
    }

    // ========== Edge Cases ==========

    public void testBuildResponseWithEmptyTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> emptyList = Collections.emptyList();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, emptyList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"status\":\"success\""));
        assertThat(responseContent, containsString("\"result\":[]"));
    }

    public void testBuildResponseWithNullAggregations() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        SearchResponse searchResponse = createSearchResponseWithNullAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"status\":\"success\""));
        assertThat(responseContent, containsString("\"result\":[]"));
    }

    public void testBuildResponseWithEmptySamples() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithEmptySamples();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"values\":[]"));
    }

    public void testBuildResponseWithNullLabels() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithNullLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"metric\""));
    }

    public void testBuildResponseWithNullAlias() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithNullAlias();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        // Should not contain __name__ when alias is null
        assertFalse(responseContent.contains("\"__name__\""));
    }

    // ========== Metadata Field Tests ==========

    public void testResponseWithoutMetadataWhenIncludeMetadataIsFalse() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        Labels labels = ByteLabels.fromMap(Map.of("test", "no_step"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 5000L, "test_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);
        assertThat(results, hasSize(1));

        // Metadata fields should NOT be present when includeMetadata is false
        assertFalse("Step field should not be present when includeMetadata is false", results.get(0).containsKey("step"));
        assertFalse("Start field should not be present when includeMetadata is false", results.get(0).containsKey("start"));
        assertFalse("End field should not be present when includeMetadata is false", results.get(0).containsKey("end"));
    }

    public void testResponseWithDifferentMetadataPerTimeSeries() throws Exception {
        // Arrange - Create multiple time series with different metadata (step, start, end)
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        Labels labels1 = ByteLabels.fromMap(Map.of("id", "1"));
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1500L, 5000L, "metric1");

        List<Sample> samples2 = List.of(new FloatSample(2000L, 20.0));
        Labels labels2 = ByteLabels.fromMap(Map.of("id", "2"));
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 2000L, 2500L, 10000L, "metric2");

        List<Sample> samples3 = List.of(new FloatSample(3000L, 30.0));
        Labels labels3 = ByteLabels.fromMap(Map.of("id", "3"));
        TimeSeries ts3 = new TimeSeries(samples3, labels3, 3000L, 3500L, 30000L, "metric3");

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, List.of(ts1, ts2, ts3));
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);
        assertThat(results, hasSize(3));

        // Verify each time series has its own metadata values
        assertEquals("First time series step", 5000, ((Number) results.get(0).get("step")).longValue());
        assertEquals("First time series start", 1000, ((Number) results.get(0).get("start")).longValue());
        assertEquals("First time series end", 1500, ((Number) results.get(0).get("end")).longValue());

        assertEquals("Second time series step", 10000, ((Number) results.get(1).get("step")).longValue());
        assertEquals("Second time series start", 2000, ((Number) results.get(1).get("start")).longValue());
        assertEquals("Second time series end", 2500, ((Number) results.get(1).get("end")).longValue());

        assertEquals("Third time series step", 30000, ((Number) results.get(2).get("step")).longValue());
        assertEquals("Third time series start", 3000, ((Number) results.get(2).get("start")).longValue());
        assertEquals("Third time series end", 3500, ((Number) results.get(2).get("end")).longValue());
    }

    // ========== Full Response Structure Tests ==========

    public void testFullResponseStructureWithSingleTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.5));
        samples.add(new FloatSample(2000L, 20.5));

        Labels labels = ByteLabels.fromMap(Map.of("region", "us-east"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "my_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Full structure validation
        Map<String, Object> parsed = validateSuccessResponse(response);
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = getResultArray(parsed);

        assertThat(results, hasSize(1));
        Map<String, Object> result = results.get(0);

        // Validate metric labels
        Map<String, String> metric = (Map<String, String>) result.get("metric");
        assertNotNull(metric);
        assertThat(metric.get("__name__"), equalTo("my_metric"));
        assertThat(metric.get("region"), equalTo("us-east"));

        // Validate values
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull(values);
        assertThat(values, hasSize(2));

        // First value: [1.0, "10.5"]
        assertThat(values.get(0).get(0), equalTo(1.0));
        assertThat(values.get(0).get(1), equalTo("10.5"));

        // Second value: [2.0, "20.5"]
        assertThat(values.get(1).get(0), equalTo(2.0));
        assertThat(values.get(1).get(1), equalTo("20.5"));

        // Validate metadata fields at time series level
        assertNotNull("Step field should be present in time series", result.get("step"));
        assertEquals(1000, ((Number) result.get("step")).longValue());
        assertNotNull("Start field should be present in time series", result.get("start"));
        assertEquals(1000L, ((Number) result.get("start")).longValue());
        assertNotNull("End field should be present in time series", result.get("end"));
        assertEquals(2000L, ((Number) result.get("end")).longValue());
    }

    public void testFullResponseStructureWithMultipleTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        Labels labels1 = ByteLabels.fromMap(Map.of("id", "1"));
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, "metric1");

        List<Sample> samples2 = List.of(new FloatSample(2000L, 20.0));
        Labels labels2 = ByteLabels.fromMap(Map.of("id", "2"));
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 2000L, 2000L, 1000L, "metric2");

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, List.of(ts1, ts2));
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = getResultArray(parsed);

        assertThat(results, hasSize(2));

        // Verify first time series
        Map<String, String> metric1 = (Map<String, String>) results.get(0).get("metric");
        assertThat(metric1.get("__name__"), equalTo("metric1"));
        assertThat(metric1.get("id"), equalTo("1"));

        // Verify second time series
        Map<String, String> metric2 = (Map<String, String>) results.get(1).get("metric");
        assertThat(metric2.get("__name__"), equalTo("metric2"));
        assertThat(metric2.get("id"), equalTo("2"));

        // Validate metadata fields at time series level
        assertEquals(1000, ((Number) results.get(0).get("step")).longValue());
        assertEquals(1000L, ((Number) results.get(0).get("start")).longValue());
        assertEquals(1000L, ((Number) results.get(0).get("end")).longValue());

        assertEquals(1000, ((Number) results.get(1).get("step")).longValue());
        assertEquals(2000L, ((Number) results.get(1).get("start")).longValue());
        assertEquals(2000L, ((Number) results.get(1).get("end")).longValue());
    }

    public void testFullErrorResponseStructure() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        SearchResponse searchResponse = createSearchResponseWithException();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        validateErrorResponse(response);

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());
        assertThat(parsed.get("status"), equalTo("error"));
        assertTrue(parsed.get("error") instanceof String);
        assertFalse(((String) parsed.get("error")).isEmpty());
    }

    // ========== Value Formatting Tests ==========

    public void testValueFormattingAsString() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        // Create time series with various numeric values
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 1.23456789));
        samples.add(new FloatSample(2000L, 0.0));
        samples.add(new FloatSample(3000L, -99.99));
        samples.add(new FloatSample(4000L, Double.MAX_VALUE));

        Labels labels = ByteLabels.emptyLabels();
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, null);
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        // Values should be formatted as strings
        assertThat(responseContent, containsString("\"1.23456789\""));
        assertThat(responseContent, containsString("\"0.0\""));
        assertThat(responseContent, containsString("\"-99.99\""));
    }

    public void testValueFormattingWithSpecialFloatValues() throws Exception {
        // Define test cases as [input value, expected Prometheus format string] pairs
        Object[][] testCases = {
            { Double.NaN, "NaN" },
            { Double.POSITIVE_INFINITY, "+Inf" },
            { Double.NEGATIVE_INFINITY, "-Inf" },
            { 0.0, "0.0" },
            { -0.0, "-0.0" },
            { 42.5, "42.5" },
            { -99.99, "-99.99" } };

        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<Sample> samples = new ArrayList<>();
        long timestamp = 1000L;
        for (Object[] testCase : testCases) {
            samples.add(new FloatSample(timestamp, (Double) testCase[0]));
            timestamp += 1000L;
        }

        Labels labels = ByteLabels.fromMap(Map.of("test", "special_values"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, timestamp - 1000L, 1000L, "special_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());

        // Verify JSON is valid and parseable
        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());
        assertNotNull(parsed);

        // Extract values from response
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = (List<Map<String, Object>>) data.get("result");
        assertThat(results, hasSize(1));

        List<List<Object>> values = (List<List<Object>>) results.get(0).get("values");
        assertThat(values, hasSize(testCases.length));

        // Verify each test case
        for (int i = 0; i < testCases.length; i++) {
            Double inputValue = (Double) testCases[i][0];
            String expectedValue = (String) testCases[i][1];
            String actualValue = (String) values.get(i).get(1);

            assertThat("Test case " + i + ": input=" + inputValue + ", expected=" + expectedValue, actualValue, equalTo(expectedValue));
        }
    }

    private SearchResponse createSearchResponseWithException() {
        return new org.opensearch.tsdb.utils.TestDataBuilder.FailingSearchResponse();
    }
}
