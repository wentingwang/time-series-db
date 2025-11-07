/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.opensearch.tsdb.framework.translators.QueryType;
import org.opensearch.tsdb.query.rest.ResolvedPartitions;

/**
 * Query configuration for time series testing.
 *
 * <p>Uses the {@link ResolvedPartitions} class with a custom deserializer
 */
public record QueryConfig(@JsonProperty("name") String name, @JsonProperty("type") QueryType type, @JsonProperty("query") String query,
    @JsonProperty("time_config") TimeConfig config, @JsonProperty("indices") String indices,
    @JsonProperty("disable_pushdown") Boolean disablePushdown,
    @JsonProperty("resolved_partitions") @JsonDeserialize(using = ResolvedPartitionsYamlAdapter.Deserializer.class) ResolvedPartitions resolvedPartitions,
    @JsonProperty("expected") ExpectedResponse expected) {

    /**
     * Get the disable pushdown flag, defaulting to false if not specified
     */
    public boolean isDisablePushdown() {
        return disablePushdown != null && disablePushdown;
    }
}
