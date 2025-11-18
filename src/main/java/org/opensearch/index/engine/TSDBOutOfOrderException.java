/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.OpenSearchException;

/**
 * Exception thrown when a sample violates the out-of-order (OOO) cutoff window.
 * This prevents creating many old chunks by rejecting samples that are too far in the past.
 */
public class TSDBOutOfOrderException extends OpenSearchException {

    public TSDBOutOfOrderException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TSDBOutOfOrderException(String msg) {
        super(msg);
    }
}
