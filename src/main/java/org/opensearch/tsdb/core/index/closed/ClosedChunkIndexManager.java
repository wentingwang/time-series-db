/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.ReaderManager;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.Constants;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Responsible for managing the closed chunk indexes. Adds chunks to the appropriate index, removes old indexes, tracks pending changes
 * and commits them in a safe manner.
 */
public class ClosedChunkIndexManager {

    // Directory to store all closed chunk indexes under
    private static final String BLOCKS_DIR = "blocks";

    // File prefix for closed chunk index directories
    private static final String BLOCK_PREFIX = "block_";

    // Glob pattern to match closed chunk index directories
    private static final String BLOCK_FILE_GLOB = BLOCK_PREFIX + "*";

    private final Logger log;

    // Directory to store ClosedChunkIndexes under
    private final Path dir;

    // Ordered map from max timestamp for the index, to the ClosedChunkIndex instance
    private final NavigableMap<Long, ClosedChunkIndex> closedChunkIndexMap;

    // Maps from ClosedChunkIndex, to MemSeries with the MaxMMapTimestamps for chunks pending commit to that index
    private final Map<ClosedChunkIndex, Map<MemSeries, Long>> pendingChunksToSeriesMMapTimestamps;

    // Thread-safety when adding/replacing indexes
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor for ClosedChunkIndexManager
     * @param dir to store blocks under
     * @param shardId ShardId for logging context
     */
    public ClosedChunkIndexManager(Path dir, ShardId shardId) {
        this.dir = dir.resolve(BLOCKS_DIR);
        try {
            Files.createDirectories(this.dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create blocks directory: " + this.dir, e);
        }

        this.log = Loggers.getLogger(ClosedChunkIndexManager.class, shardId);
        closedChunkIndexMap = new TreeMap<>();
        pendingChunksToSeriesMMapTimestamps = new HashMap<>();
        openClosedChunkIndexes(this.dir);
    }

    /**
     * Open existing ClosedChunkIndexes from disk, based on the block_ directory prefix.
     */
    private void openClosedChunkIndexes(Path dir) {
        lock.lock();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, BLOCK_FILE_GLOB)) {
            for (Path path : stream) {
                closedChunkIndexMap.put(
                    Long.parseLong(path.getFileName().toString().substring(BLOCK_PREFIX.length())),
                    new ClosedChunkIndex(path)
                );
            }
            log.info("Loaded {} blocks from {}", closedChunkIndexMap.size(), dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load closed chunk indexes", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds a MemChunk to the correct closed chunk index. If a ClosedChunkIndex does not exist for the required time range, creates a new one.
     * <p>
     * MemChunks are generally added to the latest ClosedChunkIndex, but may be added to an earlier index if the chunk's max timestamp
     * belongs in a previous block. This scenario can occur when a new block is created, but a series' data is delayed. Placing the
     * chunk in previous block ensures block boundaries are not crossed, and reduces creation of tiny chunks.
     *
     * @param series the series the chunk belongs to
     * @param chunk the chunk to add
     * @throws IOException if there is an error adding the chunk
     */
    public void addMemChunk(MemSeries series, MemChunk chunk) throws IOException {
        // chunks are nearly always added to the latest index, so optimistically reverse iterate
        Long targetIndexMaxTime = null;
        for (long indexMaxTime : closedChunkIndexMap.descendingKeySet()) {
            if (chunk.getMaxTimestamp() > indexMaxTime) {
                break;
            }
            targetIndexMaxTime = indexMaxTime;
        }

        if (targetIndexMaxTime != null) {
            ClosedChunkIndex targetIndex = closedChunkIndexMap.get(targetIndexMaxTime);
            addMemChunkToClosedChunkIndex(targetIndex, series.getLabels(), series, chunk);
            return;
        }

        ClosedChunkIndex newIndex = createNewIndex(chunk.getMaxTimestamp());
        addMemChunkToClosedChunkIndex(newIndex, series.getLabels(), series, chunk);
    }

    private void addMemChunkToClosedChunkIndex(ClosedChunkIndex closedChunkIndex, Labels labels, MemSeries series, MemChunk chunk)
        throws IOException {
        closedChunkIndex.addNewChunk(labels, chunk);
        // mark the max mmap timestamp for the series, so we can later update the series at the correct time
        pendingChunksToSeriesMMapTimestamps.computeIfAbsent(closedChunkIndex, k -> new HashMap<>())
            .compute(series, (MemSeries s, Long existingValue) -> {
                if (existingValue == null || existingValue < chunk.getMaxTimestamp()) {
                    return chunk.getMaxTimestamp();
                }
                return existingValue;
            });
    }

    private ClosedChunkIndex createNewIndex(long chunkTimestamp) throws IOException {
        long newIndexTimeBoundary = rangeForTimestamp(chunkTimestamp, Constants.Time.DEFAULT_BLOCK_DURATION);
        ClosedChunkIndex newIndex = new ClosedChunkIndex(dir.resolve(BLOCK_PREFIX + newIndexTimeBoundary));
        closedChunkIndexMap.put(newIndexTimeBoundary, newIndex);
        log.info("Created new block {}", newIndexTimeBoundary);
        return newIndex;
    }

    /**
     * Adds an OOOChunk to the correct closed chunk index.
     */
    public void addOOOChunk() {
        throw new UnsupportedOperationException("not yet implemented"); // TODO
    }

    /**
     * Calls commit on all indexes that have pending changes.
     * <p>
     * Since we need to ensure we don't replay data that has been committed, we commit changes indexes in ascending order (based
     * on timestamp). This ensures that if we crash during the process of committed indexes, each commit() operation is atomic in the
     * sense that a newly added chunk will not be re-indexed if the process is restarted.
     *
     * @param allSeries all series in the head, used to update series metadata
     */
    public void commitChangedIndexes(List<MemSeries> allSeries) {
        lock.lock();
        try {
            // commit in ascending order, using closedChunkIndexMap rather than pendingChunksToSeriesMMapTimestamps.keySet()
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                Map<MemSeries, Long> pendingSeriesMMapTimestamps = pendingChunksToSeriesMMapTimestamps.get(index);
                if (pendingSeriesMMapTimestamps == null) {
                    continue;
                }

                // update the maxMMapTimestamp for each series, based on the pending chunks that will be committed to the current index
                for (Map.Entry<MemSeries, Long> entry : pendingSeriesMMapTimestamps.entrySet()) {
                    entry.getKey().setMaxMMapTimestamp(entry.getValue());
                }

                index.commitWithMetadata(allSeries);
                pendingChunksToSeriesMMapTimestamps.remove(index);
            }
            assert pendingChunksToSeriesMMapTimestamps.isEmpty() : "pending data should be empty after commit";
        } finally {
            lock.unlock();
        }
    }

    /**
     * Updates the series in the head based on data from closed chunk indexes. Not locked, as this is called sequentially during engine init
     *
     * @param seriesUpdater SeriesUpdater used to update the series
     */
    public void updateSeriesFromCommitData(SeriesUpdater seriesUpdater) {
        for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
            index.updateSeriesFromCommitData(seriesUpdater);
        }
    }

    /**
     * Get all ReaderManagers for the closed chunk indexes.
     *
     * @return a list of ReaderManagers
     */
    public List<ReaderManager> getReaderManagers() {
        lock.lock();
        try {
            List<ReaderManager> readerManagers = new ArrayList<>();
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                readerManagers.add(index.getDirectoryReaderManager());
            }
            return readerManagers;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the number of closed chunk indexes managed.
     * @return the number of closed chunk indexes
     */
    public int getNumBlocks() {
        lock.lock();
        try {
            return closedChunkIndexMap.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Snapshot all closed chunk indexes.
     * @return a SnapshotResult containing the list of IndexCommits and release actions
     */
    public SnapshotResult snapshotAllIndexes() {
        lock.lock();
        try {
            List<IndexCommit> snapshots = new ArrayList<>();
            List<Runnable> releaseActions = new ArrayList<>();

            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                try {
                    IndexCommit snapshot = index.snapshot();
                    snapshots.add(snapshot);
                    releaseActions.add(() -> {
                        try {
                            index.release(snapshot);
                        } catch (IOException e) {
                            log.warn("Failed to release closed chunk index snapshot", e);
                        }
                    });
                } catch (IOException | IllegalStateException e) {
                    log.warn("No index commit available for snapshot in closed chunk index", e);
                }
            }

            return new SnapshotResult(snapshots, releaseActions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Result of snapshotAllIndexes, containing the list of IndexCommits and release actions.
     * @param indexCommits list of IndexCommits
     * @param releaseActions list of Runnables to release the snapshots
     */
    public record SnapshotResult(List<IndexCommit> indexCommits, List<Runnable> releaseActions) {
    }

    /**
     * Closes all indexes and releases resources.
     */
    public void close() {
        lock.lock();
        try {
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                index.close();
            }
            closedChunkIndexMap.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Calculates the end timestamp for the given timestamp based on the chunk range.
     */
    private long rangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange + chunkRange;
    }
}
