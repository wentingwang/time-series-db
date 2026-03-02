/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class LoadBlockResponseTests extends OpenSearchTestCase {

    public void testSuccessResponse() {
        LoadBlockResponse response = new LoadBlockResponse(2, List.of("block_1", "block_2"), List.of());
        assertTrue(response.isSuccess());
        assertEquals(2, response.getBlocksLoaded());
        assertEquals(List.of("block_1", "block_2"), response.getLoadedBlocks());
        assertTrue(response.getFailedBlocks().isEmpty());
        assertTrue(response.getMessage().contains("success"));
        assertNull(response.getMetrics());
    }

    public void testPartialFailureResponse() {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of("block_2 (not found)"));
        assertFalse(response.isSuccess());
        assertEquals(1, response.getBlocksLoaded());
        assertEquals(1, response.getFailedBlocks().size());
        assertNull(response.getMetrics());
    }

    public void testErrorResponse() {
        LoadBlockResponse response = new LoadBlockResponse("Index not found");
        assertFalse(response.isSuccess());
        assertEquals(0, response.getBlocksLoaded());
        assertTrue(response.getLoadedBlocks().isEmpty());
        assertEquals("Index not found", response.getMessage());
        assertNull(response.getMetrics());
    }

    public void testSerialization() throws IOException {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1_2_abc"), List.of("block_other (error)"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LoadBlockResponse read = new LoadBlockResponse(in);
                assertEquals(response.isSuccess(), read.isSuccess());
                assertEquals(response.getBlocksLoaded(), read.getBlocksLoaded());
                assertEquals(response.getLoadedBlocks(), read.getLoadedBlocks());
                assertEquals(response.getFailedBlocks(), read.getFailedBlocks());
                assertEquals(response.getMessage(), read.getMessage());
                assertNull(read.getMetrics());
            }
        }
    }

    public void testToXContent() throws IOException {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            assertTrue(json.contains("\"success\":true"));
            assertTrue(json.contains("\"blocks_loaded\":1"));
            assertTrue(json.contains("block_1"));
        }
    }

    public void testMetricsSerialization() throws IOException {
        LoadBlockResponse.BlockLoadMetrics metrics = new LoadBlockResponse.BlockLoadMetrics(
            5230,
            3100,
            800,
            1330,
            1568735,
            1572864,
            42,
            125000,
            300
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            metrics.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LoadBlockResponse.BlockLoadMetrics read = new LoadBlockResponse.BlockLoadMetrics(in);
                assertEquals(5230L, read.getTotalDurationMs());
                assertEquals(3100L, read.getDownloadDurationMs());
                assertEquals(800L, read.getExtractDurationMs());
                assertEquals(1330L, read.getLoadDurationMs());
                assertEquals(1568735L, read.getDownloadFileSizeBytes());
                assertEquals(1572864L, read.getBlockIndexSizeBytes());
                assertEquals(42, read.getChunkCount());
                assertEquals(125000L, read.getSampleCount());
                assertEquals(300, read.getSeriesCount());
            }
        }
    }

    public void testResponseWithMetricsSerialization() throws IOException {
        LoadBlockResponse.BlockLoadMetrics metrics = new LoadBlockResponse.BlockLoadMetrics(
            5230,
            3100,
            800,
            1330,
            1568735,
            1572864,
            42,
            125000,
            300
        );
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of(), metrics);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LoadBlockResponse read = new LoadBlockResponse(in);
                assertEquals(response.isSuccess(), read.isSuccess());
                assertEquals(response.getBlocksLoaded(), read.getBlocksLoaded());
                assertEquals(response.getLoadedBlocks(), read.getLoadedBlocks());
                assertEquals(response.getFailedBlocks(), read.getFailedBlocks());
                assertEquals(response.getMessage(), read.getMessage());
                assertNotNull(read.getMetrics());
                assertEquals(5230L, read.getMetrics().getTotalDurationMs());
                assertEquals(3100L, read.getMetrics().getDownloadDurationMs());
                assertEquals(800L, read.getMetrics().getExtractDurationMs());
                assertEquals(1330L, read.getMetrics().getLoadDurationMs());
                assertEquals(1568735L, read.getMetrics().getDownloadFileSizeBytes());
                assertEquals(1572864L, read.getMetrics().getBlockIndexSizeBytes());
                assertEquals(42, read.getMetrics().getChunkCount());
                assertEquals(125000L, read.getMetrics().getSampleCount());
                assertEquals(300, read.getMetrics().getSeriesCount());
            }
        }
    }

    public void testToXContentWithMetrics() throws IOException {
        LoadBlockResponse.BlockLoadMetrics metrics = new LoadBlockResponse.BlockLoadMetrics(
            5230,
            3100,
            800,
            1330,
            1568735,
            1572864,
            42,
            125000,
            300
        );
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of(), metrics);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            assertTrue(json.contains("\"success\":true"));
            assertTrue(json.contains("\"blocks_loaded\":1"));
            assertTrue(json.contains("\"metrics\":{"));
            assertTrue(json.contains("\"total_duration_ms\":5230"));
            assertTrue(json.contains("\"download_duration_ms\":3100"));
            assertTrue(json.contains("\"extract_duration_ms\":800"));
            assertTrue(json.contains("\"load_duration_ms\":1330"));
            assertTrue(json.contains("\"download_file_size_bytes\":1568735"));
            assertTrue(json.contains("\"block_index_size_bytes\":1572864"));
            assertTrue(json.contains("\"chunk_count\":42"));
            assertTrue(json.contains("\"sample_count\":125000"));
            assertTrue(json.contains("\"series_count\":300"));
        }
    }

    public void testToXContentWithoutMetrics() throws IOException {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of());

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            assertTrue(json.contains("\"success\":true"));
            assertFalse(json.contains("\"metrics\""));
        }
    }
}
