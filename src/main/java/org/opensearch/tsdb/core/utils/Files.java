/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

public class Files {

    /**
     * Recursively deletes a directory and all its contents.
     * <p>
     * This method uses a depth-first traversal with reverse ordering to ensure
     * files are deleted before their parent directories. If the directory doesn't
     * exist, the method returns without error. Care should be taken  by the caller
     * to ensure the directory is not in use.
     *</p>
     * @param delPath the path to the directory to delete
     * @throws IOException      if there's an error walking the directory tree
     * @throws RuntimeException if there's an error deleting individual files or directories
     */
    public static void deleteDirectory(Path delPath) throws IOException {
        if (!java.nio.file.Files.exists(delPath)) {
            return;
        }

        try (var s = java.nio.file.Files.walk(delPath)) {
            for (var path : s.sorted(Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.delete(path);
            }
        }
    }

    /**
     * Recursively copies a directory and all its contents.
     * <p>
     * This method creates the target directory and copies all files and subdirectories
     * from the source to the target. If the target already exists, an IOException is thrown.
     * </p>
     *
     * @param source the source directory to copy from
     * @param target the target directory to copy to
     * @throws IOException if there's an error during the copy operation
     */
    public static void copyDirectory(Path source, Path target) throws IOException {
        if (!java.nio.file.Files.exists(source)) {
            throw new IOException("Source directory does not exist: " + source);
        }

        if (!java.nio.file.Files.isDirectory(source)) {
            throw new IOException("Source is not a directory: " + source);
        }

        java.nio.file.Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // Use string-based path resolution to avoid FilterPath mismatch issues in tests
                String relativePath = source.relativize(dir).toString();
                Path targetDir = relativePath.isEmpty() ? target : target.resolve(relativePath);
                java.nio.file.Files.createDirectories(targetDir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // Use string-based path resolution to avoid FilterPath mismatch issues in tests
                String relativePath = source.relativize(file).toString();
                Path targetFile = target.resolve(relativePath);
                java.nio.file.Files.copy(file, targetFile);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
