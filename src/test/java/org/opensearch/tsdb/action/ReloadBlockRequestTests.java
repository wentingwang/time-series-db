/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ReloadBlockRequestTests extends OpenSearchTestCase {

    public void testValidateRequiresIndex() {
        ReloadBlockRequest request = new ReloadBlockRequest();
        request.setBlockName("block_1_2_abc");
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.validationErrors().stream().anyMatch(msg -> msg.contains("index")));
    }

    public void testValidateEmptyIndex() {
        ReloadBlockRequest request = new ReloadBlockRequest("");
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
    }

    public void testValidateSuccess() {
        ReloadBlockRequest request = new ReloadBlockRequest("my_index");
        assertNull(request.validate());
        request = new ReloadBlockRequest("my_index", "block_1_2_xyz");
        assertNull(request.validate());
    }

    public void testValidateSuccessWithUrl() {
        ReloadBlockRequest request = new ReloadBlockRequest("my_index");
        request.setUrl("https://terrablob.uberinternal.com/blob/preview/-/prod/personal/file.tar.gz");
        assertNull(request.validate());
    }

    public void testIsReloadAll() {
        assertTrue(new ReloadBlockRequest("idx").isReloadAll());
        assertTrue(new ReloadBlockRequest("idx", null).isReloadAll());
        assertTrue(new ReloadBlockRequest("idx", "").isReloadAll());
        assertFalse(new ReloadBlockRequest("idx", "block_1_2_abc").isReloadAll());
    }

    public void testHasUrl() {
        ReloadBlockRequest request = new ReloadBlockRequest("idx");
        assertFalse(request.hasUrl());

        request.setUrl(null);
        assertFalse(request.hasUrl());

        request.setUrl("");
        assertFalse(request.hasUrl());

        request.setUrl("https://terrablob.uberinternal.com/blob/preview/-/prod/file.tar.gz");
        assertTrue(request.hasUrl());

        request.setUrl("/data/blocks/block_123_456_uuid");
        assertTrue(request.hasUrl());
    }

    public void testSerialization() throws IOException {
        ReloadBlockRequest request = new ReloadBlockRequest("test_index", "block_100_200_uuid");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ReloadBlockRequest read = new ReloadBlockRequest(in);
                assertEquals(request.getIndex(), read.getIndex());
                assertEquals(request.getBlockName(), read.getBlockName());
                assertNull(read.getUrl());
                assertFalse(read.hasUrl());
            }
        }
    }

    public void testSerializationWithUrl() throws IOException {
        ReloadBlockRequest request = new ReloadBlockRequest("test_index");
        request.setUrl("https://terrablob.uberinternal.com/blob/preview/-/prod/file.tar.gz");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ReloadBlockRequest read = new ReloadBlockRequest(in);
                assertEquals(request.getIndex(), read.getIndex());
                assertEquals(request.getUrl(), read.getUrl());
                assertTrue(read.hasUrl());
            }
        }
    }

    public void testSerializationReloadAll() throws IOException {
        ReloadBlockRequest request = new ReloadBlockRequest("test_index");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ReloadBlockRequest read = new ReloadBlockRequest(in);
                assertEquals(request.getIndex(), read.getIndex());
                assertNull(read.getBlockName());
                assertTrue(read.isReloadAll());
                assertFalse(read.hasUrl());
            }
        }
    }

    public void testDefaultShardIsZero() {
        ReloadBlockRequest request = new ReloadBlockRequest("my_index");
        assertEquals(0, request.getShard());
    }

    public void testSetShard() {
        ReloadBlockRequest request = new ReloadBlockRequest("my_index");
        request.setShard(3);
        assertEquals(3, request.getShard());
    }

    public void testValidateNegativeShard() {
        ReloadBlockRequest request = new ReloadBlockRequest("my_index");
        request.setShard(-1);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertTrue(e.validationErrors().stream().anyMatch(msg -> msg.contains("shard must be >= 0")));
    }

    public void testSerializationWithShard() throws IOException {
        ReloadBlockRequest request = new ReloadBlockRequest("test_index");
        request.setShard(5);
        request.setUrl("https://terrablob.uberinternal.com/blob/preview/-/prod/file.tar.gz");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ReloadBlockRequest read = new ReloadBlockRequest(in);
                assertEquals(request.getIndex(), read.getIndex());
                assertEquals(5, read.getShard());
                assertEquals(request.getUrl(), read.getUrl());
            }
        }
    }
}
