/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.io.IOException;
import java.util.List;

public class NoopCompaction implements Compaction {
    @Override
    public List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes) {
        return List.of();
    }

    @Override
    public void compact(List<ClosedChunkIndex> sources, ClosedChunkIndex dest) throws IOException {

    }

    @Override
    public long getFrequency() {
        return Long.MAX_VALUE;
    }
}
