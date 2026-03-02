/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class S3BlockDownloaderTests extends OpenSearchTestCase {

    public void testParseTpathFromFullUrl() {
        String url = "https://terrablob.uberinternal.com/blob/preview/-/prod/personal/user/blocks/block_123.tar.gz";
        String tpath = S3BlockDownloader.parseTpath(url);
        assertEquals("prod/personal/user/blocks/block_123.tar.gz", tpath);
    }

    public void testParseTpathFromUrlWithDifferentPrefix() {
        String url = "https://terrablob.uberinternal.com/some/other/path/-/staging/data/file.tar.gz";
        String tpath = S3BlockDownloader.parseTpath(url);
        assertEquals("staging/data/file.tar.gz", tpath);
    }

    public void testParseTpathThrowsForInvalidUrl() {
        String url = "https://example.com/no/marker/here/file.tar.gz";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> S3BlockDownloader.parseTpath(url));
        assertTrue(e.getMessage().contains("/-/"));
    }

    public void testBlockNameFromFileName() {
        assertEquals("block_100_200_uuid", S3BlockDownloader.blockNameFromFileName("block_100_200_uuid.tar.gz"));
        assertEquals("block_100_200_uuid", S3BlockDownloader.blockNameFromFileName("block_100_200_uuid.tgz"));
        assertEquals("block_100_200_uuid", S3BlockDownloader.blockNameFromFileName("block_100_200_uuid"));
    }

    public void testFindBlockDirDirectChild() throws IOException {
        Path tempDir = createTempDir("test-extract");
        Path blockDir = Files.createDirectory(tempDir.resolve("block_100_200_abc"));
        Files.createFile(blockDir.resolve("_0.cfe"));

        Path found = S3BlockDownloader.findBlockDir(tempDir);
        assertNotNull(found);
        assertEquals("block_100_200_abc", found.getFileName().toString());
    }

    public void testFindBlockDirNestedOneLevel() throws IOException {
        Path tempDir = createTempDir("test-extract");
        Path wrapperDir = Files.createDirectory(tempDir.resolve("wrapper"));
        Path blockDir = Files.createDirectory(wrapperDir.resolve("block_100_200_abc"));
        Files.createFile(blockDir.resolve("_0.cfe"));

        Path found = S3BlockDownloader.findBlockDir(tempDir);
        assertNotNull(found);
        assertEquals("block_100_200_abc", found.getFileName().toString());
    }

    public void testFindBlockDirReturnsNullWhenNoBlockDir() throws IOException {
        Path tempDir = createTempDir("test-extract");
        Files.createDirectory(tempDir.resolve("not_a_block"));

        Path found = S3BlockDownloader.findBlockDir(tempDir);
        assertNull(found);
    }
}
