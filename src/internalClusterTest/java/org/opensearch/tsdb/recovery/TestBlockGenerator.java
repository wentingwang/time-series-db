/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.common.settings.Settings;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to generate test TSDB blocks with minimal historical data.
 * This creates reusable test blocks that can be loaded into OpenSearch for testing.
 */
public class TestBlockGenerator {

    /**
     * Generate a test block with specific timestamps.
     *
     * @param outputDir Directory where the block will be created
     * @param minTimestamp Minimum timestamp for the block
     * @param maxTimestamp Maximum timestamp for the block
     * @return Metadata of the created block
     * @throws IOException if block creation fails
     */
    public static ClosedChunkIndex.Metadata generateTestBlockWithTimestamps(Path outputDir, long minTimestamp, long maxTimestamp)
        throws IOException {
        System.out.println("==> Generating test block with fixed timestamps at: " + outputDir);
        System.out.println("==> Time range: " + minTimestamp + " to " + maxTimestamp);

        String blockDirName = "block_" + minTimestamp + "_" + maxTimestamp + "_test";
        Path blockPath = outputDir.resolve(blockDirName);

        // Clean up if exists
        if (Files.exists(blockPath)) {
            System.out.println("==> Cleaning up existing block directory");
            org.opensearch.tsdb.core.utils.Files.deleteDirectory(blockPath);
        }

        // Create block metadata
        ClosedChunkIndex.Metadata metadata = new ClosedChunkIndex.Metadata(blockDirName, minTimestamp, maxTimestamp);

        // Create settings for the block
        Settings settings = Settings.builder().put("index.tsdb_engine.labels.storage_type", "binary").build();

        // Create the ClosedChunkIndex
        System.out.println("==> Creating ClosedChunkIndex at: " + blockPath);
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(blockPath, metadata, TimeUnit.MILLISECONDS, settings);

        try {
            // Create labels for the time series
            Labels labels = ByteLabels.fromStrings("app", "test", "env", "historical");

            System.out.println("==> Creating time series with labels: " + labels);

            // Create a MemChunk with 10 samples
            int numSamples = 10;
            MemChunk memChunk = new MemChunk(numSamples, minTimestamp, maxTimestamp, null, Encoding.XOR);

            // Add samples to the chunk
            // Spread samples evenly across the time range
            long timeRangeMillis = maxTimestamp - minTimestamp;
            long sampleIntervalMillis = timeRangeMillis / (numSamples - 1);

            long timestamp = minTimestamp;
            for (int i = 0; i < numSamples; i++) {
                double value = (double) (i + 1);
                memChunk.append(timestamp, value, i);
                timestamp += sampleIntervalMillis;
            }

            System.out.println("==> Adding chunk with " + numSamples + " samples");

            // Add chunk to the index
            closedChunkIndex.addNewChunk(labels, memChunk);

            // Create a minimal series list for metadata
            List<MemSeries> seriesList = new ArrayList<>();
            MemSeries series = new MemSeries(labels.stableHash(), labels);
            series.setMaxMMapTimestamp(maxTimestamp);
            seriesList.add(series);

            // Commit the index with metadata
            System.out.println("==> Committing index");
            closedChunkIndex.commitWithMetadata(seriesList);

            System.out.println("==> Successfully created test block!");

        } catch (Exception e) {
            System.err.println("==> Error creating test block: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            try {
                closedChunkIndex.close();
                Thread.sleep(100);
            } catch (Exception e) {
                System.err.println("==> Error closing index: " + e.getMessage());
            }
        }

        return metadata;
    }

    /**
     * Generate a minimal test block with one time series and 10 samples.
     *
     * Block details:
     * - Time range: 1000000 to 1900000 (timestamps)
     * - Labels: {app="test", env="historical"}
     * - 10 samples with incrementing values
     *
     * @param outputDir Directory where the block will be created
     * @return Metadata of the created block
     * @throws IOException if block creation fails
     */
    public static ClosedChunkIndex.Metadata generateTestBlock(Path outputDir) throws IOException {
        System.out.println("==> Generating test block at: " + outputDir);

        // Block time range: 1 year ago to (1 year ago + 2 hours)
        long now = System.currentTimeMillis();
        long oneYearInMillis = 365L * 24 * 60 * 60 * 1000; // 365 days in milliseconds
        long twoHoursInMillis = 2L * 60 * 60 * 1000; // 2 hours in milliseconds

        long minTimestamp = now - oneYearInMillis;
        long maxTimestamp = minTimestamp + twoHoursInMillis;

        String blockDirName = "block_" + minTimestamp + "_" + maxTimestamp + "_test";

        System.out.println("==> Time range:");
        System.out.println("    Now: " + now + " (" + new java.util.Date(now) + ")");
        System.out.println("    Min: " + minTimestamp + " (" + new java.util.Date(minTimestamp) + ")");
        System.out.println("    Max: " + maxTimestamp + " (" + new java.util.Date(maxTimestamp) + ")");

        Path blockPath = outputDir.resolve(blockDirName);

        // Clean up if exists
        if (Files.exists(blockPath)) {
            System.out.println("==> Cleaning up existing block directory");
            org.opensearch.tsdb.core.utils.Files.deleteDirectory(blockPath);
        }

        // Create block metadata
        ClosedChunkIndex.Metadata metadata = new ClosedChunkIndex.Metadata(blockDirName, minTimestamp, maxTimestamp);

        // Create settings for the block
        Settings settings = Settings.builder().put("index.tsdb_engine.labels.storage_type", "binary").build();

        // Create the ClosedChunkIndex
        System.out.println("==> Creating ClosedChunkIndex at: " + blockPath);
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(blockPath, metadata, TimeUnit.MILLISECONDS, settings);

        try {
            // Create labels for the time series
            Labels labels = ByteLabels.fromStrings("app", "test", "env", "historical");

            System.out.println("==> Creating time series with labels: " + labels);

            // Create a MemChunk with 10 samples
            int numSamples = 13;
            MemChunk memChunk = new MemChunk(numSamples, minTimestamp, maxTimestamp, null, Encoding.XOR);

            // Add samples to the chunk
            // Spread samples evenly across the 2-hour range
            long timeRangeMillis = maxTimestamp - minTimestamp;
            long sampleIntervalMillis = timeRangeMillis / (numSamples - 1); // Divide range by (n-1) to include both endpoints

            long timestamp = minTimestamp;
            for (int i = 0; i < numSamples; i++) {
                double value = (double) (i + 1);
                memChunk.append(timestamp, value, i);
                timestamp += sampleIntervalMillis;
            }

            System.out.println("==> Adding chunk with " + numSamples + " samples");
            System.out.println("    Sample interval: ~" + (sampleIntervalMillis / 1000 / 60) + " minutes");
            System.out.println("    Values: 1.0 - " + numSamples + ".0");

            // Add chunk to the index
            closedChunkIndex.addNewChunk(labels, memChunk);

            // Create a minimal series list for metadata
            List<MemSeries> seriesList = new ArrayList<>();
            MemSeries series = new MemSeries(labels.stableHash(), labels);
            series.setMaxMMapTimestamp(maxTimestamp);
            seriesList.add(series);

            // Commit the index with metadata
            System.out.println("==> Committing index");
            closedChunkIndex.commitWithMetadata(seriesList);

            System.out.println("==> Successfully created test block!");

        } catch (Exception e) {
            System.err.println("==> Error creating test block: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            try {
                closedChunkIndex.close();
                // Give it a moment to release file locks
                Thread.sleep(100);
            } catch (Exception e) {
                System.err.println("==> Error closing index: " + e.getMessage());
            }
        }

        // Verify files were created
        System.out.println("==> Verifying created files:");
        try (var files = Files.list(blockPath)) {
            files.forEach(file -> {
                try {
                    long size = Files.isRegularFile(file) ? Files.size(file) : 0;
                    System.out.println("    " + file.getFileName() + " - " + size + " bytes");
                } catch (IOException e) {
                    System.err.println("    " + file.getFileName() + " - error reading size");
                }
            });
        }

        return metadata;
    }

    /**
     * Main method to generate test blocks.
     * Run this to create test block files in the test resources directory.
     */
    public static void main(String[] args) throws IOException {
        System.out.println("==========================================================");
        System.out.println("TSDB Test Block Generator");
        System.out.println("==========================================================");

        // Determine output directory
        Path projectRoot = Paths.get(System.getProperty("user.dir"));
        Path testResourcesDir = projectRoot.resolve("src/internalClusterTest/resources/test_blocks");

        // Create resources directory if it doesn't exist
        Files.createDirectories(testResourcesDir);

        System.out.println("Output directory: " + testResourcesDir);
        System.out.println();

        // Generate the test block
        ClosedChunkIndex.Metadata metadata = generateTestBlock(testResourcesDir);

        System.out.println();
        System.out.println("==========================================================");
        System.out.println("Test Block Generated Successfully!");
        System.out.println("==========================================================");
        System.out.println("Block directory: " + metadata.directoryName());
        System.out.println("Time range: " + metadata.minTimestamp() + " - " + metadata.maxTimestamp());
        System.out.println("Location: " + testResourcesDir.resolve(metadata.directoryName()));
        System.out.println();
        System.out.println("You can now use this block for testing historical data loading.");
        System.out.println("==========================================================");
    }
}
