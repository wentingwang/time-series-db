/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Head storage implementation for active time series data.
 *
 * This package provides the "head" storage layer that handles recently written time series data
 * before it gets moved into long-term storage blocks. The head maintains active series in memory
 * and provides fast append and query access patterns.
 *
 * Key components:
 * - Head storage management for active time series
 * - Memory-efficient series organization and indexing
 * - Integration with compression and compaction processes
 * - Fast query access to recent data
 * - Thread-safe operations for concurrent writes and reads
 */
package org.opensearch.tsdb.core.head;
