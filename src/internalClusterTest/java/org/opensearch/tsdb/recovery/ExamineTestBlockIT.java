/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test to examine the generated test block files.
 * This test generates the block and waits 60 seconds before cleanup,
 * giving you time to examine the files.
 *
 * Run with: ./gradlew :internalClusterTest --tests "org.opensearch.tsdb.recovery.ExamineTestBlockIT"
 */
public class ExamineTestBlockIT extends OpenSearchTestCase {

    public void testExamineBlock() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TSDB Test Block - File Examination");
        System.out.println("=".repeat(70));

        // Generate block in temp directory
        Path testResourcesDir = createTempDir().resolve("test_blocks");
        Files.createDirectories(testResourcesDir);

        ClosedChunkIndex.Metadata metadata = TestBlockGenerator.generateTestBlock(testResourcesDir);
        Path blockPath = testResourcesDir.resolve(metadata.directoryName());

        System.out.println("\n✓ Block generated successfully!");
        System.out.println("\n" + "=".repeat(70));
        System.out.println("EXAMINE THE FILES NOW!");
        System.out.println("=".repeat(70));
        System.out.println("\nLocation:");
        System.out.println("  " + blockPath.toAbsolutePath());
        System.out.println("\nFiles:");
        try (var files = Files.list(blockPath)) {
            files.sorted().forEach(file -> {
                try {
                    long size = Files.size(file);
                    System.out.println(String.format("  %-20s %,d bytes", file.getFileName(), size));
                } catch (Exception e) {
                    System.out.println("  " + file.getFileName() + " (error reading size)");
                }
            });
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("INSTRUCTIONS:");
        System.out.println("=".repeat(70));
        System.out.println("1. Open a new terminal window");
        System.out.println("2. Navigate to the location above");
        System.out.println("3. Examine the files with: ls -la");
        System.out.println("4. View file contents with: xxd <filename> | head");
        System.out.println("5. Press Ctrl+C in this terminal when done examining");
        System.out.println("=" + "=".repeat(70) + "\n");

        // Wait 60 seconds to allow examination
        System.out.println("Waiting 60 seconds before cleanup...\n");
        Thread.sleep(60000);

        System.out.println("Test complete. Files will be cleaned up now.");
    }
}
