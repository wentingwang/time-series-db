/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.tsdb.core.model;

import org.opensearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * MapLabels implements Labels using a HashMap for storage.
 */
public class MapLabels implements Labels {
    private final Map<String, String> labels;
    private long hash = Long.MIN_VALUE;

    /**
     * Constructs a new MapLabels instance with the specified label map.
     * 
     * @param labels the map containing label key-value pairs
     */
    public MapLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    /**
     * Creates a MapLabels instance from an array of strings representing key-value pairs.
     * 
     * @param labels array of strings in key-value pairs (e.g., "key1", "value1", "key2", "value2")
     * @return a new MapLabels instance containing the specified labels
     * @throws IllegalArgumentException if the number of strings is odd
     */
    public static MapLabels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("Labels must be in pairs");
        }
        Map<String, String> labelMap = new HashMap<>();
        for (int i = 0; i < labels.length; i += 2) {
            labelMap.put(labels[i], labels[i + 1]);
        }
        return new MapLabels(labelMap);
    }

    /**
     * Creates an empty MapLabels instance.
     * 
     * @return an empty MapLabels instance
     */
    public static MapLabels emptyLabels() {
        return new MapLabels(Map.of());
    }

    /**
     * Converts the labels to a key-value string format.
     * 
     * @return a string representation of the labels in key:value format
     */
    @Override
    public String toKeyValueString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue());
            sb.append(' ');
        }
        // Remove the trailing space if there are any labels
        if (!sb.isEmpty()) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * Returns an unmodifiable view of the labels as a Map.
     * 
     * @return an unmodifiable map view of the labels
     */
    @Override
    public Map<String, String> toMapView() {
        return Map.copyOf(labels);
    }

    /**
     * Checks if the labels collection is empty.
     * 
     * @return true if there are no labels, false otherwise
     */
    @Override
    public boolean isEmpty() {
        return labels.isEmpty();
    }

    /**
     * Retrieves the value for a specific label name.
     * 
     * @param name the label name to look up
     * @return the label value, or an empty string if the label doesn't exist
     */
    @Override
    public String get(String name) {
        return labels.getOrDefault(name, "");
    }

    /**
     * Checks if a label with the specified name exists.
     * 
     * @param name the label name to check
     * @return true if a label with the given name exists, false otherwise
     */
    @Override
    public boolean has(String name) {
        return labels.containsKey(name);
    }

    /**
     * Computes a stable hash value for the labels.
     * FIXME: this is not stable yet, need to iterate on it
     * <p>The hash is cached after the first computation for performance.</p>
     * 
     * @return a stable hash value for the labels
     */
    @Override
    public long stableHash() {
        if (hash != Long.MIN_VALUE) {
            return hash;
        }

        // combine logic from boost::hash_combine
        long combinedHash = 0;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            byte[] bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            long hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).hashCode();
            combinedHash ^= (hash + 0x9e3779b97f4a7c15L + (combinedHash << 6) + (combinedHash >> 2));
            bytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
            hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).hashCode();
            combinedHash ^= (hash + 0x9e3779b97f4a7c15L + (combinedHash << 6) + (combinedHash >> 2));
        }
        hash = combinedHash;
        return combinedHash;
    }

    /**
     * Compares this MapLabels instance with another object for equality.
     * 
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapLabels other)) return false;
        return Objects.equals(this.labels, other.labels);
    }

    /**
     * Computes the hash code for this MapLabels instance.
     * 
     * @return the hash code for this instance
     */
    @Override
    public int hashCode() {
        long stableHash = stableHash();
        return Long.hashCode(stableHash);
    }

    /**
     * Returns a string representation of this MapLabels instance.
     * 
     * @return a string representation of the labels
     */
    @Override
    public String toString() {
        return toKeyValueString();
    }
}
