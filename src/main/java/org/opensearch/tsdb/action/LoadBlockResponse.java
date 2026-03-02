/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response from loading or reloading blocks in a TSDB index.
 */
public class LoadBlockResponse extends ActionResponse implements ToXContentObject {

    private final boolean success;
    private final int blocksLoaded;
    private final List<String> loadedBlocks;
    private final List<String> failedBlocks;
    private final String message;
    private final BlockLoadMetrics metrics;

    /** @param failedBlocks block names that failed (empty means full success). */
    public LoadBlockResponse(int blocksLoaded, List<String> loadedBlocks, List<String> failedBlocks) {
        this(blocksLoaded, loadedBlocks, failedBlocks, null);
    }

    /** Full constructor with optional metrics (for URL-based loads). */
    public LoadBlockResponse(int blocksLoaded, List<String> loadedBlocks, List<String> failedBlocks, BlockLoadMetrics metrics) {
        this.success = failedBlocks.isEmpty();
        this.blocksLoaded = blocksLoaded;
        this.loadedBlocks = loadedBlocks;
        this.failedBlocks = failedBlocks;
        this.message = success ? "Blocks loaded successfully" : "Some blocks failed to load";
        this.metrics = metrics;
    }

    /** Error response with message only. */
    public LoadBlockResponse(String errorMessage) {
        this.success = false;
        this.blocksLoaded = 0;
        this.loadedBlocks = List.of();
        this.failedBlocks = List.of();
        this.message = errorMessage;
        this.metrics = null;
    }

    public LoadBlockResponse(StreamInput in) throws IOException {
        super(in);
        this.success = in.readBoolean();
        this.blocksLoaded = in.readInt();
        this.loadedBlocks = in.readStringList();
        this.failedBlocks = in.readStringList();
        this.message = in.readString();
        this.metrics = in.readBoolean() ? new BlockLoadMetrics(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeInt(blocksLoaded);
        out.writeStringCollection(loadedBlocks);
        out.writeStringCollection(failedBlocks);
        out.writeString(message);
        if (metrics != null) {
            out.writeBoolean(true);
            metrics.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("success", success);
        builder.field("blocks_loaded", blocksLoaded);
        builder.array("loaded_blocks", loadedBlocks.toArray(new String[0]));
        if (!failedBlocks.isEmpty()) {
            builder.array("failed_blocks", failedBlocks.toArray(new String[0]));
        }
        builder.field("message", message);
        if (metrics != null) {
            builder.field("metrics");
            metrics.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getBlocksLoaded() {
        return blocksLoaded;
    }

    public List<String> getLoadedBlocks() {
        return loadedBlocks;
    }

    public List<String> getFailedBlocks() {
        return failedBlocks;
    }

    public String getMessage() {
        return message;
    }

    public BlockLoadMetrics getMetrics() {
        return metrics;
    }

    /**
     * Metrics collected during a URL-based block load operation.
     */
    public static class BlockLoadMetrics implements Writeable, ToXContentObject {
        private final long totalDurationMs;
        private final long downloadDurationMs;
        private final long extractDurationMs;
        private final long loadDurationMs;
        private final long downloadFileSizeBytes;
        private final long blockIndexSizeBytes;
        private final int chunkCount;
        private final long sampleCount;
        private final int seriesCount;

        public BlockLoadMetrics(
            long totalDurationMs,
            long downloadDurationMs,
            long extractDurationMs,
            long loadDurationMs,
            long downloadFileSizeBytes,
            long blockIndexSizeBytes,
            int chunkCount,
            long sampleCount,
            int seriesCount
        ) {
            this.totalDurationMs = totalDurationMs;
            this.downloadDurationMs = downloadDurationMs;
            this.extractDurationMs = extractDurationMs;
            this.loadDurationMs = loadDurationMs;
            this.downloadFileSizeBytes = downloadFileSizeBytes;
            this.blockIndexSizeBytes = blockIndexSizeBytes;
            this.chunkCount = chunkCount;
            this.sampleCount = sampleCount;
            this.seriesCount = seriesCount;
        }

        public BlockLoadMetrics(StreamInput in) throws IOException {
            this.totalDurationMs = in.readVLong();
            this.downloadDurationMs = in.readVLong();
            this.extractDurationMs = in.readVLong();
            this.loadDurationMs = in.readVLong();
            this.downloadFileSizeBytes = in.readVLong();
            this.blockIndexSizeBytes = in.readVLong();
            this.chunkCount = in.readVInt();
            this.sampleCount = in.readVLong();
            this.seriesCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalDurationMs);
            out.writeVLong(downloadDurationMs);
            out.writeVLong(extractDurationMs);
            out.writeVLong(loadDurationMs);
            out.writeVLong(downloadFileSizeBytes);
            out.writeVLong(blockIndexSizeBytes);
            out.writeVInt(chunkCount);
            out.writeVLong(sampleCount);
            out.writeVInt(seriesCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_duration_ms", totalDurationMs);
            builder.field("download_duration_ms", downloadDurationMs);
            builder.field("extract_duration_ms", extractDurationMs);
            builder.field("load_duration_ms", loadDurationMs);
            builder.field("download_file_size_bytes", downloadFileSizeBytes);
            builder.field("block_index_size_bytes", blockIndexSizeBytes);
            builder.field("chunk_count", chunkCount);
            builder.field("sample_count", sampleCount);
            builder.field("series_count", seriesCount);
            builder.endObject();
            return builder;
        }

        public long getTotalDurationMs() {
            return totalDurationMs;
        }

        public long getDownloadDurationMs() {
            return downloadDurationMs;
        }

        public long getExtractDurationMs() {
            return extractDurationMs;
        }

        public long getLoadDurationMs() {
            return loadDurationMs;
        }

        public long getDownloadFileSizeBytes() {
            return downloadFileSizeBytes;
        }

        public long getBlockIndexSizeBytes() {
            return blockIndexSizeBytes;
        }

        public int getChunkCount() {
            return chunkCount;
        }

        public long getSampleCount() {
            return sampleCount;
        }

        public int getSeriesCount() {
            return seriesCount;
        }
    }
}
