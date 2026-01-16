/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.tsdb.utils.TSDBTestUtils.getSampleCountViaAggregation;

/**
 * Integration test for loading historical TSDB blocks into a running OpenSearch instance.
 * <p>
 * This test verifies that:
 * 1. Pre-existing block files can be loaded into an active index
 * 2. The loaded historical data becomes queryable alongside current data
 * 3. The block metadata is correctly registered in the engine
 * </p>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class LoadHistoricalBlockIT extends TSDBRecoveryITBase {

    private static final String INDEX_NAME = "load_historical_block_test";

    public void testLoadHistoricalBlock() throws Exception {
        logger.info("==> Starting node");
        String nodeName = internalCluster().startNode();

        logger.info("==> Creating TSDB index: {}", INDEX_NAME);
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        logger.info("==> Indexing current time series data");
        long now = System.currentTimeMillis();
        long currentDataStart = now - 600_000L; // Last 10 minutes
        long intervalMillis = 10_000L; // 10 second intervals
        int currentSampleCount = 50;

        // Index some current data
        List<TimeSeriesSample> currentSamples = generateTimeSeriesSamples(currentSampleCount, currentDataStart, intervalMillis);
        ingestSamples(currentSamples, INDEX_NAME);

        logger.info("==> Flushing to create closed blocks");
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Verify current data is queryable
        refresh(INDEX_NAME);
        int currentDataCount = getSampleCountViaAggregation(client(), INDEX_NAME, currentDataStart, now + intervalMillis, intervalMillis);
        logger.info("    Current data sample count: {}", currentDataCount);
        assertTrue("Should have current data", currentDataCount > 0);

        logger.info("==> Loading historical test block from resources");
        System.out.println("==> Loading historical test block from resources");

        // Load pre-existing block from resources (do NOT generate with TestBlockGenerator)
        // Gradle copies resources from src/internalClusterTest/resources/ to build/resources/internalClusterTest/
        String blockDirName = "block_1737053150272_1737060350272_test";

        // Extract timestamps from block directory name: block_<minTimestamp>_<maxTimestamp>_<suffix>
        String[] parts = blockDirName.split("_");
        long minTimestamp = Long.parseLong(parts[1]);
        long maxTimestamp = Long.parseLong(parts[2]);

        // Use build output location where Gradle copies the resources
        Path currentDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
        Path projectRoot = currentDir.getFileName().toString().equals("internalClusterTest")
            ? currentDir.getParent().getParent().getParent()
            : currentDir;
        Path historicalBlockPath = projectRoot.resolve("build/resources/internalClusterTest/test_blocks").resolve(blockDirName);

        ClosedChunkIndex.Metadata historicalMetadata = new ClosedChunkIndex.Metadata(blockDirName, minTimestamp, maxTimestamp);

        logger.info("==> Historical block path: {}", historicalBlockPath.toAbsolutePath());
        logger.info("    Time range: {} - {}", historicalMetadata.minTimestamp(), historicalMetadata.maxTimestamp());

        // Verify the block files exist in resources
        assertTrue("Historical block directory should exist in build resources", Files.exists(historicalBlockPath));
        try (var files = Files.list(historicalBlockPath)) {
            long fileCount = files.count();
            assertTrue("Should have at least 5 files in block", fileCount >= 5);
            logger.info("    Files in block: {}", fileCount);
        }

        logger.info("==> Loading historical block into the running index");

        // Get the index shard and engine
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard indexShard = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);

        // Get the engine - should be TSDBEngine
        Engine engine = getShardEngine(indexShard);
        assertNotNull("Engine should not be null", engine);
        assertTrue("Engine should be TSDBEngine", engine instanceof TSDBEngine);
        TSDBEngine tsdbEngine = (TSDBEngine) engine;

        // Load the historical block
        boolean loaded = tsdbEngine.loadHistoricalBlock(historicalBlockPath);
        assertTrue("Historical block should be loaded successfully", loaded);

        logger.info("==> Historical block loaded successfully");

        // Flush again to ensure metadata is persisted
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        refresh(INDEX_NAME);

        logger.info("==> Verifying data is queryable");

        // Query for current data (should still return results)
        int currentDataAfterLoad = getSampleCountViaAggregation(
            client(),
            INDEX_NAME,
            currentDataStart,
            now + intervalMillis,
            intervalMillis
        );
        logger.info("    Current data sample count after load: {}", currentDataAfterLoad);
        assertEquals("Current data count should remain the same", currentDataCount, currentDataAfterLoad);

        // Query for historical data (should return results from the loaded block)
        int historicalDataCount = getSampleCountViaAggregation(
            client(),
            INDEX_NAME,
            historicalMetadata.minTimestamp(),
            historicalMetadata.maxTimestamp() + 1,
            intervalMillis
        );
        logger.info("    Historical data sample count: {}", historicalDataCount);
        assertTrue("Should find historical data from loaded block", historicalDataCount > 0);

        // Wait 60 seconds to allow examination
        System.out.println("Waiting 60 seconds before cleanup...\n");
        Thread.sleep(60000);

        logger.info("==> Test completed successfully!");
        logger.info("    ✓ Historical block loaded");
        logger.info("    ✓ Current data queryable: {} samples", currentDataAfterLoad);
        logger.info("    ✓ Historical data queryable: {} samples", historicalDataCount);
    }

    /**
     * Test that attempting to load a block with an existing time range returns false.
     */
    public void testLoadDuplicateBlockReturnsFalse() throws Exception {
        logger.info("==> Starting node");
        String nodeName = internalCluster().startNode();

        logger.info("==> Creating TSDB index: {}", INDEX_NAME);
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        logger.info("==> Generating historical test block");
        Path tempDir = createTempDir().resolve("historical_blocks");
        Files.createDirectories(tempDir);

        ClosedChunkIndex.Metadata metadata = TestBlockGenerator.generateTestBlock(tempDir);
        Path blockPath = tempDir.resolve(metadata.directoryName());

        // Get the engine
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard indexShard = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        Engine engine = getShardEngine(indexShard);
        assertNotNull("Engine should not be null", engine);
        assertTrue("Engine should be TSDBEngine", engine instanceof TSDBEngine);
        TSDBEngine tsdbEngine = (TSDBEngine) engine;

        // Load the block first time - should succeed
        boolean firstLoad = tsdbEngine.loadHistoricalBlock(blockPath);
        assertTrue("First load should succeed", firstLoad);

        // Attempt to load the same block again - should return false
        boolean secondLoad = tsdbEngine.loadHistoricalBlock(blockPath);
        assertFalse("Second load of same block should return false", secondLoad);

        logger.info("==> Test completed successfully!");
    }

    /**
     * Test that invalid block directory names are rejected with proper error messages.
     */
    public void testLoadInvalidBlockThrowsException() throws Exception {
        logger.info("==> Starting node");
        String nodeName = internalCluster().startNode();

        logger.info("==> Creating TSDB index: {}", INDEX_NAME);
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        // Create a directory with invalid name
        Path tempDir = createTempDir();
        Path invalidBlockPath = tempDir.resolve("invalid_block_name");
        Files.createDirectories(invalidBlockPath);

        // Get the engine
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard indexShard = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        Engine engine = getShardEngine(indexShard);
        assertNotNull("Engine should not be null", engine);
        assertTrue("Engine should be TSDBEngine", engine instanceof TSDBEngine);
        TSDBEngine tsdbEngine = (TSDBEngine) engine;

        // Attempt to load invalid block - should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> tsdbEngine.loadHistoricalBlock(invalidBlockPath)
        );

        assertTrue("Error message should mention invalid directory name", exception.getMessage().contains("Invalid block directory name"));

        logger.info("==> Test completed successfully!");
    }
}
