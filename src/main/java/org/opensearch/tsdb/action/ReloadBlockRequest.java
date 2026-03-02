/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to reload blocks in a TSDB index.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li>Local reload (existing): reload blocks already in the engine's blocks directory</li>
 *   <li>URL download (new): download a block from terrablob or load from a local directory path</li>
 * </ul>
 * When {@code url} is provided, it takes precedence and {@code blockName} is ignored.
 */
public class ReloadBlockRequest extends ActionRequest {

    private String index;
    private String blockName;
    private String url;
    private int shard = 0;

    public ReloadBlockRequest() {}

    /** Index only: reload all local blocks. */
    public ReloadBlockRequest(String index) {
        this.index = index;
    }

    /** Index and optional block name (null = reload all). */
    public ReloadBlockRequest(String index, String blockName) {
        this.index = index;
        this.blockName = blockName;
    }

    public ReloadBlockRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.blockName = in.readOptionalString();
        this.url = in.readOptionalString();
        this.shard = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalString(blockName);
        out.writeOptionalString(url);
        out.writeVInt(shard);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (index == null || index.isEmpty()) {
            e = addValidationError("index is required", e);
        }
        if (shard < 0) {
            e = addValidationError("shard must be >= 0", e);
        }
        return e;
    }

    public String getIndex() {
        return index;
    }

    public ReloadBlockRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getBlockName() {
        return blockName;
    }

    public ReloadBlockRequest setBlockName(String blockName) {
        this.blockName = blockName;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ReloadBlockRequest setUrl(String url) {
        this.url = url;
        return this;
    }

    /** Returns true if a URL (terrablob or local path) was provided. */
    public boolean hasUrl() {
        return url != null && !url.isEmpty();
    }

    public int getShard() {
        return shard;
    }

    public ReloadBlockRequest setShard(int shard) {
        this.shard = shard;
        return this;
    }

    public boolean isReloadAll() {
        return blockName == null || blockName.isEmpty();
    }
}
