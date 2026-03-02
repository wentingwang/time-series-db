/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import com.amazonaws.services.s3.AmazonS3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Transport action to reload or download+load blocks in a TSDB index.
 * <p>
 * When a URL is provided, downloads the block (from terrablob or local path) and loads it.
 * Otherwise, reloads blocks already in the engine's blocks directory.
 */
public class TransportReloadBlockAction extends HandledTransportAction<ReloadBlockRequest, LoadBlockResponse> {

    private static final Logger logger = LogManager.getLogger(TransportReloadBlockAction.class);

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportReloadBlockAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        super(ReloadBlockAction.NAME, transportService, actionFilters, ReloadBlockRequest::new);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ReloadBlockRequest request, ActionListener<LoadBlockResponse> listener) {
        String indexName = request.getIndex();
        int shardId = request.getShard();
        try {
            ClusterState state = clusterService.state();

            // Validate index exists
            IndexMetadata indexMetadata = state.metadata().index(indexName);
            if (indexMetadata == null) {
                throw new IndexNotFoundException(indexName);
            }

            // Find primary shard for the requested shard ID
            IndexRoutingTable indexRouting = state.routingTable().index(indexName);
            if (indexRouting == null || indexRouting.shard(shardId) == null) {
                throw new IllegalArgumentException("Shard " + shardId + " does not exist for index: " + indexName);
            }
            ShardRouting primaryShard = indexRouting.shard(shardId).primaryShard();
            if (primaryShard == null || !primaryShard.assignedToNode()) {
                throw new IllegalStateException("Primary shard " + shardId + " not assigned for index: " + indexName);
            }

            // Route: local or forward
            String localNodeId = clusterService.localNode().getId();
            if (localNodeId.equals(primaryShard.currentNodeId())) {
                executeLocally(request, shardId, listener);
            } else {
                DiscoveryNode primaryNode = state.nodes().get(primaryShard.currentNodeId());
                logger.info("Forwarding reload_block to node {} (primary for shard {})", primaryNode.getName(), shardId);
                transportService.sendRequest(
                    primaryNode,
                    ReloadBlockAction.NAME,
                    request,
                    new ActionListenerResponseHandler<>(listener, LoadBlockResponse::new)
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void executeLocally(ReloadBlockRequest request, int shardId, ActionListener<LoadBlockResponse> listener) {
        String indexName = request.getIndex();
        try {
            TSDBEngine tsdbEngine = resolveEngine(indexName, shardId);

            if (request.hasUrl()) {
                handleUrlLoad(request.getUrl(), tsdbEngine, listener);
            } else {
                handleLocalReload(request, tsdbEngine, listener);
            }
        } catch (IndexNotFoundException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            logger.error("Unexpected error while reloading blocks: {}", e.getMessage(), e);
            listener.onFailure(e);
        }
    }

    private TSDBEngine resolveEngine(String indexName, int shardId) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(indexName);
        }
        Index index = indexMetadata.getIndex();

        var indexService = indicesService.indexService(index);
        if (indexService == null) {
            throw new IndexNotFoundException(indexName);
        }

        IndexShard indexShard = indexService.getShard(shardId);
        if (indexShard == null) {
            throw new IllegalStateException("Shard " + shardId + " not found for index: " + indexName);
        }

        Engine engine = getEngineFromShard(indexShard);
        if (engine == null) {
            throw new IllegalStateException("Engine not available for index: " + indexName);
        }

        if (!(engine instanceof TSDBEngine)) {
            throw new IllegalStateException("Index " + indexName + " is not a TSDB index. Engine type: " + engine.getClass().getName());
        }

        return (TSDBEngine) engine;
    }

    private void handleUrlLoad(String url, TSDBEngine tsdbEngine, ActionListener<LoadBlockResponse> listener) {
        List<String> loadedBlocks = new ArrayList<>();
        List<String> failedBlocks = new ArrayList<>();
        Path tempDir = null;

        try {
            Path sourceBlockPath;

            if (isTerraBlobUrl(url)) {
                // TerraBlob URL: download via S3 SDK through cerberus proxy, then extract
                tempDir = Files.createTempDirectory("tsdb-download-" + UUID.randomUUID());
                String tpath = S3BlockDownloader.parseTpath(url);
                String fileName = tpath.substring(tpath.lastIndexOf('/') + 1);
                String blockName = S3BlockDownloader.blockNameFromFileName(fileName);
                Path tarGzFile = tempDir.resolve(fileName);

                logger.info("Downloading block from terrablob: tpath={}", tpath);
                AmazonS3 s3 = S3BlockDownloader.createS3Client("localhost", S3BlockDownloader.DEFAULT_PORT, S3BlockDownloader.SERVICE_NAME);
                S3BlockDownloader.download(s3, S3BlockDownloader.DEFAULT_BUCKET, tpath, tarGzFile);

                logger.info("Extracting archive: {}", tarGzFile);
                sourceBlockPath = S3BlockDownloader.extractTarGz(tarGzFile, tempDir, blockName);
            } else {
                // Local directory path
                sourceBlockPath = Path.of(url);
                if (!Files.exists(sourceBlockPath)) {
                    listener.onFailure(new IOException("Local path does not exist: " + url));
                    return;
                }
                if (!Files.isDirectory(sourceBlockPath)) {
                    listener.onFailure(new IOException("Local path is not a directory: " + url));
                    return;
                }
            }

            String blockDirName = sourceBlockPath.getFileName().toString();
            logger.info("Loading block from source: {}", sourceBlockPath);

            if (tsdbEngine.addHistoricalBlock(sourceBlockPath)) {
                loadedBlocks.add(blockDirName);
            } else {
                failedBlocks.add(blockDirName + " (already loaded or overlapping time range)");
            }

            listener.onResponse(new LoadBlockResponse(loadedBlocks.size(), loadedBlocks, failedBlocks));
        } catch (Exception e) {
            logger.error("Failed to load block from URL {}: {}", url, e.getMessage(), e);
            listener.onFailure(e);
        } finally {
            if (tempDir != null) {
                cleanupTempDir(tempDir);
            }
        }
    }

    private void handleLocalReload(ReloadBlockRequest request, TSDBEngine tsdbEngine, ActionListener<LoadBlockResponse> listener) {
        List<String> loadedBlocks = new ArrayList<>();
        List<String> failedBlocks = new ArrayList<>();

        if (request.isReloadAll()) {
            try {
                List<String> reloaded = tsdbEngine.reloadAllLocalBlocks();
                loadedBlocks.addAll(reloaded);
            } catch (Exception e) {
                logger.error("Failed to reload local blocks: {}", e.getMessage(), e);
                failedBlocks.add("all (" + e.getMessage() + ")");
            }
        } else {
            String blockName = request.getBlockName();
            try {
                if (tsdbEngine.reloadLocalBlock(blockName)) {
                    loadedBlocks.add(blockName);
                } else {
                    failedBlocks.add(blockName + " (not found or already loaded)");
                }
            } catch (Exception e) {
                failedBlocks.add(blockName + " (" + e.getMessage() + ")");
                logger.error("Failed to reload block {}: {}", blockName, e.getMessage(), e);
            }
        }

        listener.onResponse(new LoadBlockResponse(loadedBlocks.size(), loadedBlocks, failedBlocks));
    }

    private static boolean isTerraBlobUrl(String url) {
        return url.startsWith("http://") || url.startsWith("https://");
    }

    private static void cleanupTempDir(Path tempDir) {
        try {
            org.opensearch.tsdb.core.utils.Files.deleteDirectory(tempDir);
        } catch (IOException e) {
            logger.warn("Failed to clean up temp directory {}: {}", tempDir, e.getMessage());
        }
    }

    /** Gets engine from shard via reflection (IndexShard.getEngineOrNull is package-private). */
    private static Engine getEngineFromShard(IndexShard indexShard) {
        try {
            java.lang.reflect.Method method = indexShard.getClass().getDeclaredMethod("getEngineOrNull");
            method.setAccessible(true);
            return (Engine) method.invoke(indexShard);
        } catch (Exception e) {
            logger.error("Failed to get engine from shard: {}", e.getMessage(), e);
            return null;
        }
    }
}
