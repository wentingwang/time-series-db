/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import java.io.IOException;
import java.util.Optional;

/**
 * The MetadataStore interface provides methods to store and retrieve simple KV metadata.
 */

public interface MetadataStore {
    /**
     * Persists metadata associated with a given key.
     *
     * @param key   The identifier for the metadata.
     * @param value The value or state to persist for the corresponding key. This can represent progress, position, or any recoverable state information.
     * @throws IOException If an I/O error occurs while persisting metadata.
     */
    void store(String key, String value) throws IOException;

    /**
     * Retrieves the metadata or state associated with the given key.
     *
     * @param key The identifier for the metadata whose value is to be retrieved.
     * @return The Optional of state previously persisted for the corresponding key, or empty Optional.
     */
    Optional<String> retrieve(String key);
}
