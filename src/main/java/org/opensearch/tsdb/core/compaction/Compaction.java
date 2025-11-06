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

public interface Compaction {
    List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes);

    void compact(List<ClosedChunkIndex> sources, ClosedChunkIndex dest) throws IOException;

    /**
     * Returns frequency in milliseconds indicating how frequent retention is scheduled to run.
     * @return long representing frequency in milliseconds.
     */
    long getFrequency();

    /**
     * Set the frequency
     * @param frequency long representing frequency in milliseconds.
     */
    default void setFrequency(long frequency) {};
}
