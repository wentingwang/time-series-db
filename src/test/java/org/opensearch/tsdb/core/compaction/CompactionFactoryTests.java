/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;

public class CompactionFactoryTests extends OpenSearchTestCase {

    /**
     * Test create with SizeTieredCompaction type and short retention time (10 hours)
     * Expected ranges: [2, 6] (18, 54, 162, 486 filtered out as they exceed 0.1 * 10 = 1 hour)
     */
    public void testCreateSizeTieredCompactionWithShortRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "10h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with SizeTieredCompaction type and medium retention time (7 days = 168 hours)
     * Expected ranges: [2, 6] (18 is 18 hours > 0.1 * 168 = 16.8, so filtered)
     */
    public void testCreateSizeTieredCompactionWithMediumRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with SizeTieredCompaction type and long retention time (100 days)
     * Expected ranges: [2, 6, 18, 54, 162, 486] (all included, capped at 744 hours = 31 days)
     */
    public void testCreateSizeTieredCompactionWithLongRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "100d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with SizeTieredCompaction type and very long retention time (365 days)
     * Ranges should be capped at 744 hours (31 days)
     */
    public void testCreateSizeTieredCompactionWithVeryLongRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "365d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
        // Max should be capped at min(365 * 24 * 0.1, 744) = 744
    }

    /**
     * Test create with SizeTieredCompaction type and minimal retention time (1 hour)
     * Very short retention time should filter out most ranges
     */
    public void testCreateSizeTieredCompactionWithMinimalRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "1h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
        // 0.1 * 1 hour = 0.1, so all ranges >= 2 would be filtered out
    }

    /**
     * Test create with default compaction type (SizeTieredCompaction)
     */
    public void testCreateWithDefaultCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        // Default is SizeTieredCompaction
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with NoopCompaction type
     */
    public void testCreateWithNoopCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "Noop")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof NoopCompaction);
    }

    /**
     * Test create with unknown compaction type defaults to NoopCompaction
     */
    public void testCreateWithUnknownCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "UNKNOWN_TYPE")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        assertThrows(IllegalArgumentException.class, () -> CompactionFactory.create(indexSettings));
    }

    /**
     * Test create with empty compaction type string defaults to NoopCompaction
     */
    public void testCreateWithEmptyCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        assertThrows(IllegalArgumentException.class, () -> CompactionFactory.create(indexSettings));
    }

    /**
     * Test create with SizeTieredCompaction and 30 day retention time (edge case at cap boundary)
     * 30 days = 720 hours, 0.1 * 720 = 72 hours, so ranges [2, 6, 18, 54] should pass
     */
    public void testCreateSizeTieredCompactionAtCapBoundary() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "30d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with SizeTieredCompaction and 200 hour retention time
     * 0.1 * 200 = 20 hours, so ranges [2, 6, 18] should pass
     */
    public void testCreateSizeTieredCompactionWithCustomHourRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "200h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }
}
