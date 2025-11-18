/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base framework for time series integration testing.
 * Provides YAML-driven test configuration for TSDB engine validation.
 *
 * <h2>Document Format</h2>
 * <p>Uses TSDBDocument format to align with production data flow:
 * <pre>
 * {
 *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
 *   "timestamp": 1234567890,                               // epoch millis
 *   "value": 100.5                                          // double value
 * }
 * </pre>
 *
 * <p>This format directly aligns with how TSDBEngine parses and indexes data, ensuring consistency
 * between test data ingestion and production data flow.
 *
 * <p>Index mapping is obtained directly from {@link Constants.Mapping#DEFAULT_INDEX_MAPPING} to ensure
 * it matches the actual TSDB engine mapping.
 *
 * <h2>Cluster Configuration</h2>
 * <p>Supports dynamic cluster creation via YAML:
 * <pre>{@code
 * test_setup:
 *   cluster_config:
 *     nodes: 3
 *   index_configs:
 *     - name: "my_index"
 *       shards: 3
 *       replicas: 1
 * }</pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * public void testMyScenario() throws Exception {
 *     loadTestConfigurationFromFile("test_cases/my_test.yaml");
 *     runBasicTest();
 * }
 * }</pre>
 *
 * @see YamlLoader
 * @see SearchQueryExecutor
 */
@SuppressWarnings("unchecked")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false, autoManageMasterNodes = true)
public abstract class TimeSeriesTestFramework extends OpenSearchIntegTestCase {

    // TODO: consider making ingestion more realistic so we do not require an extended ooo_cutoff
    private static final String DEFAULT_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        """;

    protected TestSetup testSetup;
    protected TestCase testCase;
    protected SearchQueryExecutor queryExecutor;
    protected List<IndexConfig> indexConfigs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Loads test configuration from YAML file and initializes the test cluster.
     * This method should be called at the beginning of each test method.
     *
     * @param yamlFilePath Path to the YAML test configuration file
     * @throws Exception if configuration loading or cluster setup fails
     */
    protected void loadTestConfigurationFromFile(String yamlFilePath) throws Exception {
        testSetup = YamlLoader.loadTestSetup(yamlFilePath);
        testCase = YamlLoader.loadTestCase(yamlFilePath);

        startClusterNodes();
        initializeComponents();
        clearIndexIfExists();
    }

    private void startClusterNodes() throws Exception {
        int nodesToStart = 1;

        if (testSetup != null && testSetup.clusterConfig() != null) {
            nodesToStart = testSetup.clusterConfig().getNodes();
        }

        if (nodesToStart > 0) {
            internalCluster().startNodes(nodesToStart);
            ensureStableCluster(nodesToStart);
        }
    }

    private void initializeComponents() {
        // Create the index config by merging test config with framework defaults
        try {
            // Parse the default settings YAML
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> defaultSettings = yamlMapper.readValue(DEFAULT_INDEX_SETTINGS_YAML, Map.class);

            // Get the actual TSDB mapping from Constants (same as engine uses)
            // This ensures test mapping matches production mapping
            Map<String, Object> defaultMapping = parseMappingFromConstants();

            // Initialize index configs list
            indexConfigs = new ArrayList<>();

            // Validate that test setup and index configs are present
            if (testSetup == null) {
                throw new IllegalStateException("Test setup is required but was null");
            }

            if (testSetup.indexConfigs() == null || testSetup.indexConfigs().isEmpty()) {
                throw new IllegalStateException("Test setup must specify at least one index configuration in index_configs");
            }

            // Validate and create index configs
            for (IndexConfig testIndexConfig : testSetup.indexConfigs()) {
                if (testIndexConfig.name() == null || testIndexConfig.name().trim().isEmpty()) {
                    throw new IllegalArgumentException("Index configuration must specify a non-empty index name");
                }

                String indexName = testIndexConfig.name();
                int shards = testIndexConfig.shards();
                int replicas = testIndexConfig.replicas();
                indexConfigs.add(new IndexConfig(indexName, shards, replicas, defaultSettings, defaultMapping));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index config", e);
        }

        // Initialize query executor after successful config creation
        queryExecutor = new SearchQueryExecutor(client());
    }

    /**
     * Parse the TSDB engine's default mapping from Constants.
     * This ensures we use the exact same mapping as the engine.
     */
    private Map<String, Object> parseMappingFromConstants() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
        return jsonMapper.readValue(mappingJson, Map.class);
    }

    protected void runBasicTest() throws Exception {
        ingestTestData();
        executeAndValidateQueries();
    }

    protected void ingestTestData() throws Exception {

        // Create all indices
        for (IndexConfig indexConfig : indexConfigs) {
            createTimeSeriesIndex(indexConfig);
        }

        ensureGreen();

        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            for (InputDataConfig inputDataConfig : testCase.inputDataList()) {
                List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
                ingestSamples(samples, inputDataConfig.indexName());
            }
        }

        for (IndexConfig indexConfig : indexConfigs) {
            refresh(indexConfig.name());
        }
    }

    protected void executeAndValidateQueries() throws Exception {
        queryExecutor.executeAndValidateQueries(testCase);
    }

    protected void clearIndexIfExists() throws Exception {
        for (IndexConfig indexConfig : indexConfigs) {
            String indexName = indexConfig.name();
            if (client().admin().indices().prepareExists(indexName).get().isExists()) {
                client().admin().indices().prepareDelete(indexName).get();
                assertBusy(
                    () -> { assertFalse("Index should be deleted", client().admin().indices().prepareExists(indexName).get().isExists()); }
                );
            }
        }
    }

    protected void createTimeSeriesIndex(IndexConfig indexConfig) throws Exception {
        // Merge default settings with index configuration
        Map<String, Object> allSettings = new HashMap<>(indexConfig.settings());
        allSettings.put("index.number_of_shards", indexConfig.shards());
        allSettings.put("index.number_of_replicas", indexConfig.replicas());

        Settings settings = Settings.builder().loadFromMap(allSettings).build();
        Map<String, Object> mappingConfig = indexConfig.mapping();

        client().admin().indices().prepareCreate(indexConfig.name()).setSettings(settings).setMapping(mappingConfig).get();
    }

    /**
     * Ingest time series samples using TSDBDocument format.
     * This method minimizes duplication with TSDBEngine by using the same document format
     * that TSDBEngine expects and parses.
     *
     * <p>The TSDBDocument format used here is:
     * <pre>
     * {
     *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
     *   "timestamp": 1234567890,                               // epoch millis
     *   "value": 100.5                                          // double value
     * }
     * </pre>
     *
     * <p>This directly maps to how TSDBEngine's TSDBDocument.fromParsedDocument() expects data,
     * ensuring that test ingestion follows the same code path as production ingestion.
     *
     * @param samples The list of samples to ingest
     * @param indexName The name of the index to ingest data into
     * @throws Exception if ingestion fails
     */
    protected void ingestSamples(List<TimeSeriesSample> samples, String indexName) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();

        // Send one document per sample to match production data shape
        for (TimeSeriesSample sample : samples) {
            Map<String, String> labels = sample.labels();
            ByteLabels byteLabels = ByteLabels.fromMap(labels);

            // Create TSDBDocument format JSON using the utility method
            // This ensures consistency with TSDBEngine's document parsing
            String documentJson = TSDBTestUtils.createTSDBDocumentJson(sample);
            String seriesId = byteLabels.toString();

            IndexRequest request = new IndexRequest(indexName).source(documentJson, XContentType.JSON).routing(seriesId);
            bulkRequest.add(request);
        }

        // Execute bulk request
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException("Bulk ingestion failed: " + bulkResponse.buildFailureMessage());
            }
        }

        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        if (testSetup != null && testSetup.nodeSettings() != null) {
            for (Map.Entry<String, Object> entry : testSetup.nodeSettings().entrySet()) {
                settingsBuilder.put(entry.getKey(), entry.getValue().toString());
            }
        }

        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return super.nodePlugins();
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Disable MockEngineFactoryPlugin to avoid conflicts with TSDBEngine
        return false;
    }

    /**
     * Validates shard distribution across cluster nodes.
     *
     * @param indexConfig The index configuration
     */
    protected void validateShardDistribution(IndexConfig indexConfig) {
        String indexName = indexConfig.name();
        int expectedShards = indexConfig.shards();
        int expectedReplicas = indexConfig.replicas();

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().setIndices(indexName).get();

        IndexRoutingTable indexRoutingTable = stateResponse.getState().routingTable().index(indexName);

        assertNotNull("Index routing table should exist", indexRoutingTable);
        assertEquals("Number of shards mismatch", expectedShards, indexRoutingTable.shards().size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            assertNotNull("Shard routing table should exist for shard " + shardId, shardRoutingTable);

            int expectedCopies = 1 + expectedReplicas; // primary + replicas
            assertEquals("Shard " + shardId + " should have " + expectedCopies + " copies", expectedCopies, shardRoutingTable.size());

            // Check that all shard copies are active
            for (ShardRouting shardRouting : shardRoutingTable) {
                assertTrue("Shard " + shardId + " should be active: " + shardRouting, shardRouting.active());
            }
        }

    }

    /**
     * Returns document count per shard.
     *
     * @param indexConfig The index configuration
     * @return Map of shard ID to document count
     */
    protected Map<Integer, Long> getShardDocumentCounts(IndexConfig indexConfig) throws Exception {
        String indexName = indexConfig.name();
        Map<Integer, Long> shardCounts = new HashMap<>();

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();
        IndexStats indexStats = statsResponse.getIndex(indexName);

        if (indexStats == null) {
            return shardCounts;
        }

        for (IndexShardStats shardStats : indexStats.getIndexShards().values()) {
            int shardId = shardStats.getShardId().id();
            for (ShardStats shard : shardStats.getShards()) {
                if (shard.getShardRouting().primary()) {
                    long docCount = shard.getStats().getDocs().getCount();
                    shardCounts.put(shardId, docCount);
                    break;
                }
            }
        }

        return shardCounts;
    }

    /**
     * Validates minimum document count per shard.
     *
     * @param indexConfig The index configuration
     * @param minDocsPerShard Minimum documents per shard
     */
    protected void validateDataDistribution(IndexConfig indexConfig, int minDocsPerShard) throws Exception {
        Map<Integer, Long> shardCounts = getShardDocumentCounts(indexConfig);
        int expectedShards = indexConfig.shards();

        assertEquals("Should have document counts for all shards", expectedShards, shardCounts.size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            long count = shardCounts.getOrDefault(shardId, 0L);
            assertTrue("Shard " + shardId + " has " + count + " documents, expected at least " + minDocsPerShard, count >= minDocsPerShard);
        }

    }

    /**
     * Validates that all shards contain at least one document.
     *
     * @param indexConfig The index configuration
     */
    protected void validateAllShardsHaveData(IndexConfig indexConfig) throws Exception {
        validateDataDistribution(indexConfig, 1);
    }

}
