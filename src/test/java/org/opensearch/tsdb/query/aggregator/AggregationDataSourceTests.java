/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class AggregationDataSourceTests extends OpenSearchTestCase {

    public void testConstructorAndAccessors() {
        List<String> origins = List.of("prometheus");
        List<AggregationDataSource.IndexInfo> indexes = List.of(new AggregationDataSource.IndexInfo("2d", "10s"));
        AggregationDataSource ds = new AggregationDataSource(origins, indexes);

        assertEquals(List.of("prometheus"), ds.origins());
        assertEquals(1, ds.indexes().size());
        assertEquals("2d", ds.indexes().get(0).index());
        assertEquals("10s", ds.indexes().get(0).stepSize());
    }

    public void testEmpty() {
        AggregationDataSource empty = AggregationDataSource.EMPTY;

        assertTrue(empty.origins().isEmpty());
        assertTrue(empty.indexes().isEmpty());
    }

    public void testWriteToAndReadFrom() throws IOException {
        AggregationDataSource original = new AggregationDataSource(
            List.of("prometheus", "graphite"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"), new AggregationDataSource.IndexInfo("30d", "1m"))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        AggregationDataSource deserialized = new AggregationDataSource(in);

        assertEquals(original, deserialized);
    }

    public void testWriteToAndReadFromEmpty() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AggregationDataSource.EMPTY.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        AggregationDataSource deserialized = new AggregationDataSource(in);

        assertEquals(AggregationDataSource.EMPTY, deserialized);
    }

    public void testMerge() {
        AggregationDataSource ds1 = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );
        AggregationDataSource ds2 = new AggregationDataSource(
            List.of("graphite"),
            List.of(new AggregationDataSource.IndexInfo("30d", "1m"))
        );

        AggregationDataSource merged = ds1.merge(ds2);

        assertEquals(List.of("prometheus", "graphite"), merged.origins());
        assertEquals(2, merged.indexes().size());
        assertEquals("2d", merged.indexes().get(0).index());
        assertEquals("30d", merged.indexes().get(1).index());
    }

    public void testMergeDeduplicatesOrigins() {
        AggregationDataSource ds1 = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );
        AggregationDataSource ds2 = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("30d", "1m"))
        );

        AggregationDataSource merged = ds1.merge(ds2);

        assertEquals(List.of("prometheus"), merged.origins());
        assertEquals(2, merged.indexes().size());
    }

    public void testMergeDeduplicatesIndexes() {
        AggregationDataSource.IndexInfo idx = new AggregationDataSource.IndexInfo("2d", "10s");
        AggregationDataSource ds1 = new AggregationDataSource(List.of("prometheus"), List.of(idx));
        AggregationDataSource ds2 = new AggregationDataSource(List.of("prometheus"), List.of(idx));

        AggregationDataSource merged = ds1.merge(ds2);

        assertEquals(1, merged.indexes().size());
        assertEquals("2d", merged.indexes().get(0).index());
        assertEquals("10s", merged.indexes().get(0).stepSize());
    }

    public void testMergeWithEmpty() {
        AggregationDataSource ds = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );

        AggregationDataSource mergedLeft = AggregationDataSource.EMPTY.merge(ds);
        assertEquals(ds, mergedLeft);

        AggregationDataSource mergedRight = ds.merge(AggregationDataSource.EMPTY);
        assertEquals(ds, mergedRight);
    }

    public void testEqualsAndHashCode() {
        AggregationDataSource ds1 = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );
        AggregationDataSource ds2 = new AggregationDataSource(
            List.of("prometheus"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );
        AggregationDataSource ds3 = new AggregationDataSource(
            List.of("graphite"),
            List.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );

        assertEquals(ds1, ds2);
        assertEquals(ds1.hashCode(), ds2.hashCode());
        assertNotEquals(ds1, ds3);
    }

    public void testIndexInfoEqualsAndHashCode() {
        AggregationDataSource.IndexInfo idx1 = new AggregationDataSource.IndexInfo("2d", "10s");
        AggregationDataSource.IndexInfo idx2 = new AggregationDataSource.IndexInfo("2d", "10s");
        AggregationDataSource.IndexInfo idx3 = new AggregationDataSource.IndexInfo("30d", "1m");

        assertEquals(idx1, idx2);
        assertEquals(idx1.hashCode(), idx2.hashCode());
        assertNotEquals(idx1, idx3);
    }
}
