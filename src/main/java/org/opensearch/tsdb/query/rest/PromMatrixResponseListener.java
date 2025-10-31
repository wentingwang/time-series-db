/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.utils.ProfileInfoMapper;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper;

import java.io.IOException;
import java.util.Objects;

/**
 * Transforms OpenSearch search responses into Prometheus-compatible matrix format.
 *
 * <p>This listener processes {@link SearchResponse} objects containing time series aggregations
 * and converts them into a matrix-formatted JSON response following the Prometheus query_range API structure.
 * The matrix format is particularly useful for representing time series data with multiple data points
 * over time.</p>
 *
 * <h2>Response Structure:</h2>
 * <pre>{@code
 * {
 *   "status": "success",
 *   "data": {
 *     "resultType": "matrix",
 *     "result": [
 *       {
 *         "metric": {
 *           "__name__": "metric_name",
 *           "label1": "value1",
 *           "label2": "value2"
 *         },
 *         "values": [
 *           [timestamp1, "value1"],
 *           [timestamp2, "value2"]
 *         ]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <h2>Error Handling:</h2>
 * <p>If an exception occurs during transformation, the listener returns a 500 error with an error response:</p>
 * <pre>{@code
 * {
 *   "status": "error",
 *   "error": "error message"
 * }
 * }</pre>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * RestChannel channel = ...;
 * String aggregationName = "timeseries_agg";
 * PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, aggregationName);
 * client.search(request, listener);
 * }</pre>
 *
 * @see RestToXContentListener
 * @see TimeSeries
 */
public class PromMatrixResponseListener extends RestToXContentListener<SearchResponse> {

    // Response field names
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_RESULT_TYPE = "resultType";
    private static final String FIELD_RESULT = "result";
    private static final String FIELD_ERROR = "error";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_VALUES = "values";

    // Response status values
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";

    // Response type values
    private static final String RESULT_TYPE_MATRIX = "matrix";

    // Prometheus label names
    private static final String LABEL_NAME = "__name__";

    private final String finalAggregationName;

    private final boolean profile;

    /**
     * Creates a new matrix response listener.
     *
     * @param channel the REST channel to send the response to
     * @param finalAggregationName the name of the final aggregation to extract (must not be null)
     * @throws NullPointerException if finalAggregationName is null
     */
    public PromMatrixResponseListener(RestChannel channel, String finalAggregationName, boolean profile) {
        super(channel);
        this.finalAggregationName = Objects.requireNonNull(finalAggregationName, "finalAggregationName cannot be null");
        this.profile = profile;
    }

    /**
     * Builds the REST response from a search response by transforming it into matrix format.
     *
     * <p>This method is called by the framework when a successful search response is received.
     * It transforms the response into matrix format and returns a {@link BytesRestResponse}.</p>
     *
     * @param response the search response to transform
     * @param builder the XContent builder to use for constructing the response
     * @return a REST response with the transformed data
     * @throws Exception if an error occurs during transformation
     */
    @Override
    public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
        try {
            transformToMatrixResponse(response, builder);
            return new BytesRestResponse(RestStatus.OK, builder);
        } catch (Exception e) {
            // Create a new builder for error response since the original builder may be in an invalid state
            XContentBuilder errorBuilder = channel.newErrorBuilder();
            return buildErrorResponse(errorBuilder, e);
        }
    }

    /**
     * Transforms a search response into matrix format and writes it to the XContent builder.
     *
     * @param response the search response containing time series aggregations
     * @param builder the XContent builder to write the matrix response to
     * @throws IOException if an I/O error occurs during writing
     */
    private void transformToMatrixResponse(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_SUCCESS);
        builder.startObject(FIELD_DATA);
        builder.field(FIELD_RESULT_TYPE, RESULT_TYPE_MATRIX);
        builder.field(
            FIELD_RESULT,
            TimeSeriesOutputMapper.extractAndTransformToPromMatrix(response.getAggregations(), finalAggregationName)
        );
        builder.endObject();
        if (profile) {
            ProfileInfoMapper.extractProfileInfo(response, builder);
        }
        builder.endObject();
    }

    /**
     * Builds an error response when transformation fails.
     *
     * @param builder the XContent builder to use for the error response
     * @param error the exception that caused the error
     * @return a REST response with error details
     * @throws IOException if an I/O error occurs during writing
     */
    private RestResponse buildErrorResponse(XContentBuilder builder, Exception error) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_ERROR);
        builder.field(FIELD_ERROR, error.getMessage());
        builder.endObject();
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
    }
}
