/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/***
 * In memory implementation of the MetadataStore, strictly for use in testing.
 */
public class InMemoryMetadataStore implements MetadataStore {
    private final Map<String, String> store = new HashMap<>();

    @Override
    public void store(String key, String value) throws IOException {
        store.put(key, value);
    }

    @Override
    public Optional<String> retrieve(String key) {
        return store.get(key) == null ? Optional.empty() : Optional.of(store.get(key));
    }
}
