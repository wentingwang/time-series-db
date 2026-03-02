/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.GZIPInputStream;

/**
 * Utility to download block archives via S3-compatible APIs and extract them.
 * <p>
 * Uses the AWS S3 SDK with an HTTP proxy (e.g. cerberus sidecar at localhost:19617)
 * to access S3-compatible storage (TerraBlob, AWS S3, MinIO, etc.).
 * <p>
 * TerraBlob URLs look like:
 * {@code https://terrablob.uberinternal.com/blob/preview/-/prod/personal/user/file.tar.gz}
 * <p>
 * The tpath (e.g. {@code prod/personal/.../file.tar.gz}) becomes the S3 object key,
 * and the bucket defaults to {@link #DEFAULT_BUCKET}.
 */
public class S3BlockDownloader {

    private static final Logger logger = LogManager.getLogger(S3BlockDownloader.class);

    static final int DEFAULT_PORT = 19617;
    static final String DEFAULT_BUCKET = "terrablob";
    static final String SERVICE_NAME = "opensearch-tsdb";
    private static final String TERRABLOB_PATH_MARKER = "/-/";
    private static final String BLOCK_DIR_PREFIX = "block_";

    private S3BlockDownloader() {}

    /**
     * Create an S3 client configured to use an HTTP proxy (e.g. cerberus sidecar).
     * <p>
     * The proxy handles authentication; the service name is passed as the AWS access key
     * (used as {@code rpc-caller} identity) and the secret key is empty.
     *
     * @param proxyHost   the proxy hostname (e.g. "localhost")
     * @param proxyPort   the proxy port (e.g. 19617)
     * @param serviceName the service identity (e.g. "opensearch-tsdb")
     * @return a configured AmazonS3 client
     */
    public static AmazonS3 createS3Client(String proxyHost, int proxyPort, String serviceName) {
        ClientConfiguration cc = new ClientConfiguration().withProxyHost(proxyHost)
            .withProxyPort(proxyPort)
            .withProtocol(Protocol.HTTP)
            .withMaxErrorRetry(3)
            .withSocketTimeout(300_000)
            .withRequestTimeout(300_000);

        return AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_WEST_2)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(serviceName, "")))
            .withClientConfiguration(cc)
            .withPathStyleAccessEnabled(true)
            .build();
    }

    /**
     * Download an object from S3-compatible storage to a local file.
     *
     * @param s3         the S3 client
     * @param bucket     the bucket name (e.g. "terrablob")
     * @param key        the object key (e.g. "prod/personal/.../file.tar.gz")
     * @param outputFile where to save the downloaded file
     */
    public static void download(AmazonS3 s3, String bucket, String key, Path outputFile) throws IOException {
        logger.info("Downloading s3://{}/{} -> {}", bucket, key, outputFile);
        Files.createDirectories(outputFile.getParent());
        try (InputStream in = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent()) {
            Files.copy(in, outputFile, StandardCopyOption.REPLACE_EXISTING);
        }
        logger.info("Downloaded {} bytes to {}", Files.size(outputFile), outputFile);
    }

    /**
     * Parse the tpath from a full terrablob URL.
     * <p>
     * Input:  {@code https://terrablob.uberinternal.com/blob/preview/-/prod/personal/.../file.tar.gz}
     * Output: {@code prod/personal/.../file.tar.gz}
     *
     * @param url the full terrablob URL
     * @return the tpath portion
     * @throws IllegalArgumentException if the URL does not contain the expected marker
     */
    public static String parseTpath(String url) {
        int markerIdx = url.indexOf(TERRABLOB_PATH_MARKER);
        if (markerIdx < 0) {
            throw new IllegalArgumentException("URL does not contain terrablob path marker '/-/': " + url);
        }
        return url.substring(markerIdx + TERRABLOB_PATH_MARKER.length());
    }

    /**
     * Extract a tar.gz file and return the path to the block directory.
     * <p>
     * Handles two archive layouts:
     * <ul>
     *   <li>Archive contains a {@code block_*} subdirectory — returns that directory</li>
     *   <li>Archive extracts flat (files directly) — wraps files in a directory named {@code blockName}</li>
     * </ul>
     *
     * @param tarGzFile the .tar.gz file to extract
     * @param outputDir the directory to extract into
     * @param blockName the block directory name to use if the archive extracts flat
     * @return the path to the block directory (block_minTs_maxTs_uuid)
     * @throws IOException if extraction fails
     */
    public static Path extractTarGz(Path tarGzFile, Path outputDir, String blockName) throws IOException {
        // Extract into a staging subdirectory first to detect layout
        Path stagingDir = outputDir.resolve("staging");
        Files.createDirectories(stagingDir);
        logger.info("Extracting {} to {}", tarGzFile, stagingDir);

        try (
            InputStream fileIn = Files.newInputStream(tarGzFile);
            BufferedInputStream buffIn = new BufferedInputStream(fileIn);
            GZIPInputStream gzipIn = new GZIPInputStream(buffIn);
            TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)
        ) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextEntry()) != null) {
                Path entryPath = stagingDir.resolve(entry.getName()).normalize();
                // Guard against zip-slip
                if (!entryPath.startsWith(stagingDir)) {
                    throw new IOException("Tar entry outside target dir: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(entryPath);
                } else {
                    Files.createDirectories(entryPath.getParent());
                    Files.copy(tarIn, entryPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }

        logger.info("Extraction complete: {}", stagingDir);

        // Check if archive contained a block_* directory
        Path blockDir = findBlockDir(stagingDir);
        if (blockDir != null) {
            return blockDir;
        }

        // Flat layout: files extracted directly into stagingDir. Rename to blockName.
        Path namedBlockDir = outputDir.resolve(blockName);
        Files.move(stagingDir, namedBlockDir);
        logger.info("Archive extracted flat, renamed staging dir to {}", namedBlockDir);
        return namedBlockDir;
    }

    /**
     * Derive a block directory name from a terrablob filename.
     * <p>
     * Input:  {@code block_1618531200000_1618962600000_glacierprod6.tar.gz}
     * Output: {@code block_1618531200000_1618962600000_glacierprod6}
     */
    public static String blockNameFromFileName(String fileName) {
        String name = fileName;
        // Strip .tar.gz or .tgz suffix
        if (name.endsWith(".tar.gz")) {
            name = name.substring(0, name.length() - 7);
        } else if (name.endsWith(".tgz")) {
            name = name.substring(0, name.length() - 4);
        }
        return name;
    }

    /**
     * Find a block_* directory inside a directory. Returns null if not found.
     * Searches direct children and one level deeper.
     */
    static Path findBlockDir(Path dir) throws IOException {
        // Check direct children
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry) && entry.getFileName().toString().startsWith(BLOCK_DIR_PREFIX)) {
                    return entry;
                }
            }
        }

        // Check one level deeper (in case archive has a wrapper directory)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    try (DirectoryStream<Path> subStream = Files.newDirectoryStream(entry)) {
                        for (Path subEntry : subStream) {
                            if (Files.isDirectory(subEntry) && subEntry.getFileName().toString().startsWith(BLOCK_DIR_PREFIX)) {
                                return subEntry;
                            }
                        }
                    }
                }
            }
        }

        return null;
    }
}
