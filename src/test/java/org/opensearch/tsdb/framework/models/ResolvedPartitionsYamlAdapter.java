/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.opensearch.tsdb.query.rest.ResolvedPartitions;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.PartitionWindow;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.ResolvedPartition;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.RoutingKey;
import org.opensearch.tsdb.utils.TimestampUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * <p>This adapter handles the conversion from YAML test format to {@link ResolvedPartitions}:
 * <ul>
 *   <li>String timestamps ("2025-01-01T00:00:00Z") or epoch millis or relative timestamps → long epoch milliseconds</li>
 *   <li>String partition_keys ("service:service1,region:us-west") → List&lt;RoutingKey&gt;</li>
 *   <li>YAML field names → production field names</li>
 * </ul>
 *
 * <p>Example YAML format:
 * <pre>{@code
 * resolved_partitions:
 *   - fetch: "fetch name:requests"
 *     windows:
 *       - partition: "metrics_index1"
 *         start: "2025-01-01T00:00:00Z"
 *         end: "2025-01-01T00:30:00Z"
 *         partition_keys: "service:service1,region:us-west"
 * }</pre>
 */
public class ResolvedPartitionsYamlAdapter {

    /**
     * YAML-friendly representation that matches the test YAML structure
     */
    public record YamlResolvedPartition(@JsonProperty("fetch") String fetch, @JsonProperty("windows") List<YamlPartitionWindow> windows) {
    }

    /**
     * YAML-friendly window with string timestamps and partition keys
     */
    public record YamlPartitionWindow(@JsonProperty("partition") String partition, @JsonProperty("start") String start,
        @JsonProperty("end") String end, @JsonProperty("partition_keys") String partitionKeys) {
    }

    /**
     * Custom deserializer that converts YAML format to main {@link ResolvedPartitions}
     */
    public static class Deserializer extends JsonDeserializer<ResolvedPartitions> {

        // Error message constants
        private static final String ERROR_INVALID_PARTITION_KEY_FORMAT = "Invalid partition key format: '%s'. Expected format: 'key:value'";
        private static final String PARTITION_KEY_SEPARATOR = ",";
        private static final String KEY_VALUE_SEPARATOR = ":";
        private static final int EXPECTED_KEY_VALUE_PARTS = 2;

        @Override
        public ResolvedPartitions deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            // Parse YAML format
            YamlResolvedPartition[] yamlPartitions = parser.readValueAs(YamlResolvedPartition[].class);

            if (yamlPartitions == null || yamlPartitions.length == 0) {
                return new ResolvedPartitions(List.of());
            }

            // Convert to main production classes
            List<ResolvedPartition> partitions = new ArrayList<>(yamlPartitions.length);
            for (YamlResolvedPartition yamlPartition : yamlPartitions) {
                partitions.add(convertPartition(yamlPartition));
            }

            return new ResolvedPartitions(partitions);
        }

        private ResolvedPartition convertPartition(YamlResolvedPartition yamlPartition) {
            List<PartitionWindow> windows = new ArrayList<>();

            if (yamlPartition.windows != null) {
                for (YamlPartitionWindow yamlWindow : yamlPartition.windows) {
                    windows.add(convertWindow(yamlWindow));
                }
            }

            return new ResolvedPartition(yamlPartition.fetch, windows);
        }

        private PartitionWindow convertWindow(YamlPartitionWindow yamlWindow) {
            // Convert string timestamps to epoch milliseconds
            long startMs = parseTimestamp(yamlWindow.start);
            long endMs = parseTimestamp(yamlWindow.end);

            // Parse comma-separated partition keys into RoutingKey objects
            List<RoutingKey> routingKeys = parsePartitionKeys(yamlWindow.partitionKeys);

            return new PartitionWindow(yamlWindow.partition, startMs, endMs, routingKeys);
        }

        private long parseTimestamp(String timestamp) {
            if (timestamp == null || timestamp.isEmpty()) {
                return 0;
            }
            return TimestampUtils.parseTimestamp(timestamp).toEpochMilli();
        }

        private List<RoutingKey> parsePartitionKeys(String partitionKeys) {
            if (partitionKeys == null || partitionKeys.trim().isEmpty()) {
                return List.of();
            }

            List<RoutingKey> routingKeyList = new ArrayList<>();
            String[] pairs = partitionKeys.split(PARTITION_KEY_SEPARATOR);

            for (String pair : pairs) {
                String[] keyValue = pair.split(KEY_VALUE_SEPARATOR, EXPECTED_KEY_VALUE_PARTS);
                if (keyValue.length == EXPECTED_KEY_VALUE_PARTS) {
                    routingKeyList.add(new RoutingKey(keyValue[0].trim(), keyValue[1].trim()));
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ROOT, ERROR_INVALID_PARTITION_KEY_FORMAT, pair));
                }
            }

            return routingKeyList;
        }
    }
}
