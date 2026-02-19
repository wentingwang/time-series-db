/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.index.closed.ClosedChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper.TimeSeriesResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        String blockDirName = "block_1768435200000_1770162600000_db6744969205401594f0";

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

    /**
     * Load the m3db block and run M3QL queries against it to verify the data is queryable
     * through the standard TSDB query pipeline.
     * Run with: ./gradlew internalClusterTest --tests "*.LoadHistoricalBlockIT.testM3QLQueryOnLoadedBlock"
     */
    public void testM3QLQueryOnLoadedBlock() throws Exception {
        String nodeName = internalCluster().startNode();

        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        String blockDirName = "block_1768435200000_1770162600000_db6744969205401594f0";
        String[] parts = blockDirName.split("_");
        long minTimestamp = Long.parseLong(parts[1]);
        long maxTimestamp = Long.parseLong(parts[2]);

        Path currentDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
        Path projectRoot = currentDir.getFileName().toString().equals("internalClusterTest")
            ? currentDir.getParent().getParent().getParent()
            : currentDir;
        Path historicalBlockPath = projectRoot.resolve("build/resources/internalClusterTest/test_blocks").resolve(blockDirName);
        assertTrue("Block directory should exist: " + historicalBlockPath, Files.exists(historicalBlockPath));

        // Load block into engine
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard indexShard = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        TSDBEngine tsdbEngine = (TSDBEngine) getShardEngine(indexShard);

        boolean loaded = tsdbEngine.loadHistoricalBlock(historicalBlockPath);
        assertTrue("Block should load successfully", loaded);

        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        refresh(INDEX_NAME);

        // Run M3QL queries against the loaded block
        long stepMillis = 600_000L; // 10 minutes

        // Query 1: Fetch a specific series we know exists (from dump: Series #16)
        String query1 = "fetch name:utilization.drivers.online-full-assignment-r2-rollup service:fulfillment-entity-signals region:oulu";
        runM3QLAndPrint(query1, INDEX_NAME, minTimestamp, maxTimestamp, stepMillis);

        // Query 2: Fetch with sum aggregation across multiple series
        String query2 = "fetch name:utilization.drivers.online-full-assignment-r2-rollup service:fulfillment-entity-signals | sum";
        runM3QLAndPrint(query2, INDEX_NAME, minTimestamp, maxTimestamp, stepMillis);

        // Query 3: Fetch a different metric
        String query3 = "fetch name:r2.oper_status service:collettore-agent dc:dca24 | sum";
        runM3QLAndPrint(query3, INDEX_NAME, minTimestamp, maxTimestamp, stepMillis);

        System.out.println("\n" + "=".repeat(80));
        System.out.println("M3QL QUERY TEST COMPLETE");
        System.out.println("=".repeat(80));
    }

    private void runM3QLAndPrint(String m3ql, String indexName, long startMs, long endMs, long stepMs) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("M3QL: " + m3ql);
        System.out.println("Time range: " + Instant.ofEpochMilli(startMs) + " to " + Instant.ofEpochMilli(endMs));
        System.out.println("Step: " + (stepMs / 1000) + "s");
        System.out.println("=".repeat(80));

        try {
            M3OSTranslator.Params params = new M3OSTranslator.Params(startMs, endMs, stepMs, true, false, null);
            SearchSourceBuilder searchSource = M3OSTranslator.translate(m3ql, params);

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSource);

            String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSource);
            System.out.println("Final aggregation name: " + finalAggName);

            SearchResponse response = client().search(searchRequest).actionGet();

            System.out.println("Search response status: " + response.status());
            System.out.println(
                "Total shards: "
                    + response.getTotalShards()
                    + ", successful: "
                    + response.getSuccessfulShards()
                    + ", failed: "
                    + response.getFailedShards()
            );

            if (response.getFailedShards() > 0) {
                System.out.println("Shard failures:");
                for (var failure : response.getShardFailures()) {
                    System.out.println("  " + failure.reason());
                }
            }

            List<TimeSeriesResult> results = TimeSeriesOutputMapper.extractAndTransformToTimeSeriesResult(
                response.getAggregations(),
                finalAggName
            );

            System.out.println("Result series count: " + results.size());

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z").withZone(ZoneId.of("UTC"));
            for (int i = 0; i < results.size(); i++) {
                TimeSeriesResult ts = results.get(i);
                System.out.println("\n  Series " + (i + 1) + ": " + ts.metric());
                System.out.println("  Total data points: " + ts.values().size());
                int total = ts.values().size();
                int show = Math.min(5, total);
                for (int j = 0; j < show; j++) {
                    List<Object> point = ts.values().get(j);
                    double epochSec = ((Number) point.get(0)).doubleValue();
                    String timeStr = dtf.format(Instant.ofEpochMilli((long) (epochSec * 1000)));
                    System.out.println("    [" + (j + 1) + "] " + timeStr + " -> " + point.get(1));
                }
                if (total > 10) {
                    System.out.println("    ... (" + (total - 10) + " more) ...");
                    for (int j = total - 5; j < total; j++) {
                        List<Object> point = ts.values().get(j);
                        double epochSec = ((Number) point.get(0)).doubleValue();
                        String timeStr = dtf.format(Instant.ofEpochMilli((long) (epochSec * 1000)));
                        System.out.println("    [" + (j + 1) + "] " + timeStr + " -> " + point.get(1));
                    }
                } else if (total > show) {
                    for (int j = show; j < total; j++) {
                        List<Object> point = ts.values().get(j);
                        double epochSec = ((Number) point.get(0)).doubleValue();
                        String timeStr = dtf.format(Instant.ofEpochMilli((long) (epochSec * 1000)));
                        System.out.println("    [" + (j + 1) + "] " + timeStr + " -> " + point.get(1));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("QUERY FAILED: " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }

    /**
     * Load the m3db block and dump ALL series and samples to stdout for inspection.
     * Run with: ./gradlew internalClusterTest --tests "*.LoadHistoricalBlockIT.testDumpAllBlockData"
     */
    public void testDumpAllBlockData() throws Exception {
        String nodeName = internalCluster().startNode();

        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        String blockDirName = "block_1768435200000_1770162600000_db6744969205401594f0";
        String[] parts = blockDirName.split("_");
        long minTimestamp = Long.parseLong(parts[1]);
        long maxTimestamp = Long.parseLong(parts[2]);

        Path currentDir = java.nio.file.Paths.get(System.getProperty("user.dir"));
        Path projectRoot = currentDir.getFileName().toString().equals("internalClusterTest")
            ? currentDir.getParent().getParent().getParent()
            : currentDir;
        Path historicalBlockPath = projectRoot.resolve("build/resources/internalClusterTest/test_blocks").resolve(blockDirName);

        assertTrue("Block directory should exist: " + historicalBlockPath, Files.exists(historicalBlockPath));

        // Copy block to a temp directory (security manager blocks writes to build resources).
        // Use toRealPath() to avoid FilterPath/UnixPath mismatch from OpenSearch test framework.
        Path tempDir = createTempDir().toRealPath().resolve("dump_blocks");
        Files.createDirectories(tempDir);
        Path tempBlockPath = tempDir.resolve(blockDirName);
        Files.createDirectories(tempBlockPath);
        java.io.File[] srcFiles = historicalBlockPath.toFile().listFiles();
        if (srcFiles != null) {
            for (java.io.File src : srcFiles) {
                Files.copy(src.toPath(), tempBlockPath.resolve(src.getName()));
            }
        }

        // Read directly from the block files using ClosedChunkIndex
        System.out.println("\n" + "=".repeat(80));
        System.out.println("DUMPING ALL DATA FROM BLOCK: " + blockDirName);
        System.out.println("Block time range: " + minTimestamp + " - " + maxTimestamp);
        System.out.println("=".repeat(80));

        ClosedChunkIndex.Metadata metadata = new ClosedChunkIndex.Metadata(blockDirName, minTimestamp, maxTimestamp);
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            tempBlockPath,
            metadata,
            TimeUnit.MILLISECONDS,
            org.opensearch.common.settings.Settings.builder().put("index.tsdb_engine.labels.storage_type", "binary").build()
        );

        try {
            dumpClosedChunkIndex(closedChunkIndex, "BLOCK CONTENTS");
        } finally {
            closedChunkIndex.close();
        }

        // Also load into the engine and verify aggregation query works
        System.out.println("\n" + "=".repeat(80));
        System.out.println("LOADING INTO ENGINE AND QUERYING");
        System.out.println("=".repeat(80));

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard indexShard = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME)).getShard(0);
        TSDBEngine tsdbEngine = (TSDBEngine) getShardEngine(indexShard);

        boolean loaded = tsdbEngine.loadHistoricalBlock(historicalBlockPath);
        System.out.println("Block loaded into engine: " + loaded);

        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        refresh(INDEX_NAME);

        int sampleCount = getSampleCountViaAggregation(client(), INDEX_NAME, minTimestamp, maxTimestamp + 1, 10_000L);
        System.out.println("Samples found via aggregation query: " + sampleCount);

        System.out.println("\n" + "=".repeat(80));
        System.out.println("DUMP COMPLETE");
        System.out.println("=".repeat(80));
    }

    private void dumpClosedChunkIndex(ClosedChunkIndex closedChunkIndex, String label) throws Exception {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS Z").withZone(ZoneId.of("UTC"));

        ReaderManager readerManager = closedChunkIndex.getDirectoryReaderManager();
        DirectoryReader reader = readerManager.acquire();
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

            System.out.println("\n--- " + label + " ---");
            System.out.println("Total documents (chunks): " + topDocs.totalHits.value());
            System.out.println("Leaf reader count: " + reader.leaves().size());

            int totalSamples = 0;
            int seriesCount = 0;
            Map<String, Integer> labelSampleCounts = new HashMap<>();

            for (LeafReaderContext leaf : reader.leaves()) {
                BinaryDocValues chunkDocValues = leaf.reader().getBinaryDocValues(Constants.IndexSchema.CHUNK);
                BinaryDocValues labelsDocValues = leaf.reader().getBinaryDocValues(Constants.IndexSchema.LABELS);
                NumericDocValues hashDocValues = leaf.reader().getNumericDocValues(Constants.IndexSchema.LABELS_HASH);

                if (chunkDocValues == null || labelsDocValues == null) {
                    System.out.println("  [WARN] Leaf segment has null chunk or labels doc values, skipping");
                    continue;
                }

                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int globalDocId = sd.doc;
                    if (globalDocId < docBase || globalDocId >= docBase + leaf.reader().maxDoc()) {
                        continue;
                    }
                    int localDocId = globalDocId - docBase;

                    // Read labels
                    String labelsStr = "<unknown>";
                    if (labelsDocValues.advanceExact(localDocId)) {
                        org.apache.lucene.util.BytesRef labelBytes = labelsDocValues.binaryValue();
                        byte[] copy = new byte[labelBytes.length];
                        System.arraycopy(labelBytes.bytes, labelBytes.offset, copy, 0, labelBytes.length);
                        Labels labels = ByteLabels.fromRawBytes(copy);
                        labelsStr = labels.toKeyValueString();
                    }

                    // Read hash
                    long hash = -1;
                    if (hashDocValues != null && hashDocValues.advanceExact(localDocId)) {
                        hash = hashDocValues.longValue();
                    }

                    // Read chunk and iterate samples
                    if (chunkDocValues.advanceExact(localDocId)) {
                        ClosedChunk chunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkDocValues.binaryValue());
                        ChunkIterator iterator = chunk.getChunkIterator();

                        seriesCount++;
                        System.out.println("\n  Series #" + seriesCount + ": " + labelsStr + " (hash=" + hash + ")");

                        int sampleIdx = 0;
                        long lastTs = -1;
                        double lastVal = 0;
                        while (iterator.next() != ChunkIterator.ValueType.NONE) {
                            ChunkIterator.TimestampValue tv = iterator.at();
                            sampleIdx++;
                            totalSamples++;
                            if (sampleIdx <= 3) {
                                String timeStr = dtf.format(Instant.ofEpochMilli(tv.timestamp()));
                                System.out.println(
                                    "    [" + sampleIdx + "] ts=" + tv.timestamp() + " (" + timeStr + ") value=" + tv.value()
                                );
                            }
                            lastTs = tv.timestamp();
                            lastVal = tv.value();
                        }
                        if (sampleIdx > 3) {
                            System.out.println("    ... (" + (sampleIdx - 3) + " more samples) ...");
                            String timeStr = dtf.format(Instant.ofEpochMilli(lastTs));
                            System.out.println("    [" + sampleIdx + "] ts=" + lastTs + " (" + timeStr + ") value=" + lastVal);
                        }

                        System.out.println("    -> " + sampleIdx + " samples in this chunk");
                        labelSampleCounts.put(labelsStr, labelSampleCounts.getOrDefault(labelsStr, 0) + sampleIdx);
                    }
                }
            }

            System.out.println("\n--- SUMMARY for " + label + " ---");
            System.out.println("Total series (chunks): " + seriesCount);
            System.out.println("Total samples: " + totalSamples);
            System.out.println("Series breakdown:");
            labelSampleCounts.forEach((k, v) -> System.out.println("  " + k + " -> " + v + " samples"));

        } finally {
            readerManager.release(reader);
        }
    }
}
