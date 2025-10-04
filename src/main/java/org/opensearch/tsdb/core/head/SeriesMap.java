/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.common.util.concurrent.ConcurrentHashMapLong;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A collection of series. Supports adding and removing series, and well as getting series by reference ({@link MemSeries#getReference()}).
 */
public class SeriesMap {

    // TODO: Consider using a custom concurrent long-keyed map implementation to avoid boxing
    private final ConcurrentHashMapLong<MemSeries> seriesMap;

    /**
     * Constructs a new SeriesMap instance.
     */
    public SeriesMap() {
        seriesMap = new ConcurrentHashMapLong<>(new ConcurrentHashMap<>());
    }

    /**
     * Get a series by its stable hash.
     * @param reference the stable hash
     * @return the MemSeries instance, or null if not found
     */
    public MemSeries getByReference(long reference) {
        return seriesMap.get(reference);
    }

    /**
     * Returns a list containing a snapshot of the current series.
     * @return list of MemSeries
     */
    public List<MemSeries> getSeriesMap() {
        return new ArrayList<>(seriesMap.values());
    }

    /**
     * Add or update a series in the collection.
     * @param series the MemSeries instance to add or update
     */
    public void add(MemSeries series) {
        seriesMap.put(series.getReference(), series);
    }

    /**
     * Add a series to the collection only if no series with the same reference exists.
     * @param series the MemSeries instance to add
     * @return the existing series if one was already present, or the new series if it was added
     */
    public MemSeries putIfAbsent(MemSeries series) {
        MemSeries existing = seriesMap.putIfAbsent(series.getReference(), series);
        return existing != null ? existing : series;
    }

    /**
     * Delete a series from the collection.
     * @param series the MemSeries instance to delete
     */
    public void delete(MemSeries series) {
        seriesMap.remove(series.getReference());
    }

    /**
     * Returns the number of series in the collection.
     * @return the number of series
     */
    public int size() {
        return seriesMap.size();
    }
}
