/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

/**
 * Integration test for multi-index scenarios with data migration.
 *
 * Tests query execution when data is split across multiple indices,
 * simulating scenarios where time series data is migrated between indices.
 * Validates that resolved partitions correctly route queries to appropriate
 * indices based on time windows.
 */
public class MultiIndexDataMigrationRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_YAML = "test_cases/multi_index_data_migration_rest_it.yaml";

    /**
     * Tests moving window aggregation across data split between two indices.
     *
     * Validates:
     * - Data entirely in single index produces correct results
     * - Data migrated between indices (with resolved partitions) produces identical results
     * - Resolved partitions correctly route time windows to appropriate indices
     */
    public void testDataMigrationWithMovingSum() throws Exception {
        initializeTest(TEST_YAML);
        runBasicTest();
    }
}
