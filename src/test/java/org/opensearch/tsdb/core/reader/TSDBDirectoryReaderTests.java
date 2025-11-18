/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.tsdb.core.mapping.Constants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBDirectoryReader
 */
@SuppressForbidden(reason = "reference counting is required here")
public class TSDBDirectoryReaderTests extends OpenSearchTestCase {

    private Directory liveDirectory;
    private Directory closedDirectory1;
    private Directory closedDirectory2;
    private Directory closedDirectory3;
    private IndexWriter liveWriter;
    private DirectoryReader liveReader;
    private DirectoryReader closedReader1;
    private DirectoryReader closedReader2;
    private DirectoryReader closedReader3;
    private TSDBDirectoryReader tsdbDirectoryReader;
    private ClosedChunkIndexManager closedChunkIndexManager;
    private MemChunkReader memChunkReader;
    private ReaderManager closedReaderManager1;
    private ReaderManager closedReaderManager2;
    private ReaderManager closedReaderManager3;
    private Map<Long, List<MemChunk>> referenceToMemChunkMap;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create test directories and readers using ByteBuffersDirectory (modern replacement for RAMDirectory)
        liveDirectory = new ByteBuffersDirectory();
        closedDirectory1 = new ByteBuffersDirectory();
        closedDirectory2 = new ByteBuffersDirectory();
        closedDirectory3 = new ByteBuffersDirectory();
        closedChunkIndexManager = mock(ClosedChunkIndexManager.class);

        // Initialize memChunks list for testing
        referenceToMemChunkMap = this.getMemChunksForLiveIndex();
        memChunkReader = (reference) -> referenceToMemChunkMap.getOrDefault(reference, new ArrayList<>())
            .stream()
            .map(MemChunk::getCompoundChunk)
            .flatMap(compoundChunk -> compoundChunk.getChunkIterators().stream())
            .collect(Collectors.toList());

        // Create some test documents for live index (similar to LiveSeriesIndex)
        liveWriter = new IndexWriter(liveDirectory, new IndexWriterConfig(new WhitespaceAnalyzer()));

        liveWriter.addDocument(getLiveDoc("service=api,env=prod", 1001L, 1000000L, 1999999L));
        liveWriter.addDocument(getLiveDoc("service=db,env=prod", 1002L, 2000000L, 2999999L)); // duplicate chunk exist in cci , assuming
                                                                                              // that this chunk is already mmaped
        liveWriter.commit();

        // Create test documents for first closed index (similar to ClosedChunkIndex)
        IndexWriter closedWriter1 = new IndexWriter(closedDirectory1, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter1.addDocument(getClosedDoc("service=api,env=prod", 1000000L, 1999999L));
        closedWriter1.addDocument(getClosedDoc("service=db,env=prod", 2000000L, 2999999L));
        closedWriter1.commit();
        closedWriter1.close();

        // Create test documents for second closed index
        IndexWriter closedWriter2 = new IndexWriter(closedDirectory2, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter2.addDocument(getClosedDoc("service=api,env=prod", 3000000L, 3999999L));
        closedWriter2.addDocument(getClosedDoc("service=cache,env=prod", 4000000L, 4999999L));
        closedWriter2.commit();
        closedWriter2.close();

        // Create test documents for third closed index
        IndexWriter closedWriter3 = new IndexWriter(closedDirectory3, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter3.addDocument(getClosedDoc("service=db,env=prod", 5000000L, 5999999L));
        closedWriter3.addDocument(getClosedDoc("service=auth,env=prod", 6000000L, 6999999L));
        closedWriter3.addDocument(getClosedDoc("service=api,env=prod", 7000000L, 7999999L));
        closedWriter3.commit();
        closedWriter3.close();

        // Open readers
        liveReader = DirectoryReader.open(liveWriter); // Create reader from IndexWriter for live updates
        closedReader1 = DirectoryReader.open(closedDirectory1);
        closedReader2 = DirectoryReader.open(closedDirectory2);
        closedReader3 = DirectoryReader.open(closedDirectory3);

        // Create ReaderManagers for closed readers
        closedReaderManager1 = new ReaderManager(closedReader1);
        closedReaderManager2 = new ReaderManager(closedReader2);
        closedReaderManager3 = new ReaderManager(closedReader3);
    }

    @After
    public void tearDown() throws Exception {
        if (tsdbDirectoryReader != null) {
            tsdbDirectoryReader.close();
        }
        if (liveWriter != null) {
            liveWriter.close();
        }
        if (liveReader != null) {
            liveReader.close();
        }
        if (closedReader1 != null) {
            closedReader1.close();
        }
        if (closedReader2 != null) {
            closedReader2.close();
        }
        if (closedReader3 != null) {
            closedReader3.close();
        }
        if (liveDirectory != null) {
            liveDirectory.close();
        }
        if (closedDirectory1 != null) {
            closedDirectory1.close();
        }
        if (closedDirectory2 != null) {
            closedDirectory2.close();
        }
        if (closedDirectory3 != null) {
            closedDirectory3.close();
        }
        // Note: ReaderManagers are closed automatically when their underlying readers are closed
        // We don't need to explicitly close them to avoid AlreadyClosedException
        super.tearDown();
    }

    @Test
    public void testConstructorWithSingleClosedReader() throws IOException {
        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosedRefCount = closedReader1.getRefCount();

        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1), memChunkReader);

        // Reference counts should be incremented by the constructor
        assertEquals("Live reader reference count should be incremented by 1", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals("Closed reader reference count should be incremented by 1", initialClosedRefCount + 1, closedReader1.getRefCount());
        assertEquals("Should have 1 closed chunk reader", 1, tsdbDirectoryReader.getClosedChunkReadersCount());
    }

    @Test
    public void testConstructorWithMultipleClosedReaders() throws IOException {
        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosed1RefCount = closedReader1.getRefCount();
        int initialClosed2RefCount = closedReader2.getRefCount();
        int initialClosed3RefCount = closedReader3.getRefCount();

        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Reference counts should be incremented by the constructor
        assertEquals("Live reader reference count should be incremented by 1", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals(
            "First closed reader reference count should be incremented by 1",
            initialClosed1RefCount + 1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Second closed reader reference count should be incremented by 1",
            initialClosed2RefCount + 1,
            closedReader2.getRefCount()
        );
        assertEquals(
            "Third closed reader reference count should be incremented by 1",
            initialClosed3RefCount + 1,
            closedReader3.getRefCount()
        );
        assertEquals("Should have 3 closed chunk readers", 3, tsdbDirectoryReader.getClosedChunkReadersCount());
    }

    @Test
    public void testLeavesCombination() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Get expected leaf count
        int expectedLiveLeaves = liveReader.leaves().size();
        int expectedClosed1Leaves = closedReader1.leaves().size();
        int expectedClosed2Leaves = closedReader2.leaves().size();
        int expectedClosed3Leaves = closedReader3.leaves().size();
        int expectedTotalLeaves = expectedLiveLeaves + expectedClosed1Leaves + expectedClosed2Leaves + expectedClosed3Leaves;

        List<LeafReaderContext> actualLeaves = tsdbDirectoryReader.leaves();

        assertEquals("Combined leaves should equal sum of all readers", expectedTotalLeaves, actualLeaves.size());
    }

    @Test
    public void testMaxDocCombination() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        int expectedMaxDoc = liveReader.maxDoc() + closedReader1.maxDoc() + closedReader2.maxDoc() + closedReader3.maxDoc();
        int actualMaxDoc = tsdbDirectoryReader.maxDoc();

        assertEquals("MaxDoc should be sum of all readers", expectedMaxDoc, actualMaxDoc);
    }

    @Test
    public void testNumDocsCombination() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        int expectedNumDocs = liveReader.numDocs() + closedReader1.numDocs() + closedReader2.numDocs() + closedReader3.numDocs();
        int actualNumDocs = tsdbDirectoryReader.numDocs();

        assertEquals("NumDocs should be sum of all readers", expectedNumDocs, actualNumDocs);
    }

    @Test
    public void testVersionCombination() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // With the new versioning system, TSDBDirectoryReader uses its own version counter
        // Default constructor without version parameter should initialize to 0
        long actualVersion = tsdbDirectoryReader.getVersion();
        assertEquals("Version should be 0 for default constructor", 0L, actualVersion);

        // Test with explicit version
        TSDBDirectoryReader versionedReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader,
            5L
        );
        assertEquals("Version should match constructor parameter", 5L, versionedReader.getVersion());
        versionedReader.close();
    }

    @Test
    public void testIsCurrentCombination() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        boolean expectedIsCurrent = liveReader.isCurrent()
            && closedReader1.isCurrent()
            && closedReader2.isCurrent()
            && closedReader3.isCurrent();
        boolean actualIsCurrent = tsdbDirectoryReader.isCurrent();

        assertEquals("IsCurrent should be AND of all readers", expectedIsCurrent, actualIsCurrent);
    }

    @Test
    public void testReferenceCountingOperations() throws IOException {
        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosed1RefCount = closedReader1.getRefCount();
        int initialClosed2RefCount = closedReader2.getRefCount();
        int initialClosed3RefCount = closedReader3.getRefCount();

        // Note : Ref count of MDR and the underlying readers are not necessarily matching because
        // The underlying readers ref counts are managed externally by their ReaderManagers.

        // Test initial reference count (should be 1 for all sub readers)
        assertEquals("Initial reference count should be 1 for live reader", 1, initialLiveRefCount);
        assertEquals("Initial reference count should be 1 for first closed reader", 1, initialClosed1RefCount);
        assertEquals("Initial reference count should be 1 for second closed reader", 1, initialClosed2RefCount);
        assertEquals("Initial reference count should be 1 for third closed reader", 1, initialClosed3RefCount);

        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );
        assertEquals("Initial reference count should be 1", 1, tsdbDirectoryReader.getRefCount());
        assertEquals("Live reader reference count should be incremented by 1", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals(
            "First closed reader reference count should be incremented by 1",
            initialClosed1RefCount + 1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Second closed reader reference count should be incremented by 1",
            initialClosed2RefCount + 1,
            closedReader2.getRefCount()
        );
        assertEquals(
            "Third closed reader reference count should be incremented by 1",
            initialClosed3RefCount + 1,
            closedReader3.getRefCount()
        );

        // Test incRef
        tsdbDirectoryReader.incRef();
        assertEquals("Reference count should be 2 after incRef", 2, tsdbDirectoryReader.getRefCount());
        assertEquals("Live reader reference count should remain unchanged", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals(
            "First closed reader reference count should remain unchanged",
            initialClosed1RefCount + 1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Second closed reader reference count should remain unchanged",
            initialClosed2RefCount + 1,
            closedReader2.getRefCount()
        );
        assertEquals(
            "Third closed reader reference count should remain unchanged",
            initialClosed3RefCount + 1,
            closedReader3.getRefCount()
        );

        // Test tryIncRef
        assertTrue("tryIncRef should return true for open reader", tsdbDirectoryReader.tryIncRef());
        assertEquals("Reference count should be 3 after tryIncRef", 3, tsdbDirectoryReader.getRefCount());

        // Test decRef (bring it back to 1)
        tsdbDirectoryReader.decRef();
        tsdbDirectoryReader.decRef();
        assertEquals("Reference count should be 1 after two decRefs", 1, tsdbDirectoryReader.getRefCount());
        assertEquals("Live reader reference count should remain unchanged", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals(
            "First closed reader reference count should remain unchanged",
            initialClosed1RefCount + 1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Second closed reader reference count should remain unchanged",
            initialClosed2RefCount + 1,
            closedReader2.getRefCount()
        );
        assertEquals(
            "Third closed reader reference count should remain unchanged",
            initialClosed3RefCount + 1,
            closedReader3.getRefCount()
        );

        tsdbDirectoryReader.close();
        assertEquals("Reference count should be 0 after close", 0, tsdbDirectoryReader.getRefCount());
        assertEquals("Live reader reference count should be decremented by 1", initialLiveRefCount, liveReader.getRefCount());
        assertEquals("First closed reader reference count should be decremented by 1", initialClosed1RefCount, closedReader1.getRefCount());
        assertEquals(
            "Second closed reader reference count should be decremented by 1",
            initialClosed2RefCount,
            closedReader2.getRefCount()
        );
        assertEquals("Third closed reader reference count should be decremented by 1", initialClosed3RefCount, closedReader3.getRefCount());
        // Here the underlying readers can still have ref count > 0 because they are managed by their ReaderManagers
    }

    @Test
    public void testDoCloseWillDecrementUnderlyingReadersRefCount() {
        try {
            int initialLiveRefCount = liveReader.getRefCount();
            int initialClosed1RefCount = closedReader1.getRefCount();
            int initialClosed2RefCount = closedReader2.getRefCount();
            int initialClosed3RefCount = closedReader3.getRefCount();

            tsdbDirectoryReader = new TSDBDirectoryReader(
                liveReader,
                Arrays.asList(closedReader1, closedReader2, closedReader3),
                memChunkReader
            );

            // Verify ref counts were incremented during construction
            assertEquals("Live reader ref count should be incremented by 1", initialLiveRefCount + 1, liveReader.getRefCount());
            assertEquals(
                "First closed reader ref count should be incremented by 1",
                initialClosed1RefCount + 1,
                closedReader1.getRefCount()
            );
            assertEquals(
                "Second closed reader ref count should be incremented by 1",
                initialClosed2RefCount + 1,
                closedReader2.getRefCount()
            );
            assertEquals(
                "Third closed reader ref count should be incremented by 1",
                initialClosed3RefCount + 1,
                closedReader3.getRefCount()
            );

            tsdbDirectoryReader.close();

            // After closing TSDBDirectoryReader, the underlying readers ref counts should be decremented back to original
            assertEquals(
                "Live reader ref count should be back to initial value after TSDBDirectoryReader is closed",
                initialLiveRefCount,
                liveReader.getRefCount()
            );
            assertEquals(
                "First closed reader ref count should be back to initial value after TSDBDirectoryReader is closed",
                initialClosed1RefCount,
                closedReader1.getRefCount()
            );
            assertEquals(
                "Second closed reader ref count should be back to initial value after TSDBDirectoryReader is closed",
                initialClosed2RefCount,
                closedReader2.getRefCount()
            );
            assertEquals(
                "Third closed reader ref count should be back to initial value after TSDBDirectoryReader is closed",
                initialClosed3RefCount,
                closedReader3.getRefCount()
            );
        } catch (IOException e) {
            fail("IOException should not be thrown: " + e.getMessage());
        }
    }

    @Test
    public void testDoOpenIfChangedWithNoChanges() throws IOException {
        // Set up mock to return the same reader managers that match the closed readers
        when(closedChunkIndexManager.getReaderManagers()).thenReturn(
            Arrays.asList(closedReaderManager1, closedReaderManager2, closedReaderManager3)
        );

        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Test when no changes occurred
        DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);

        assertNull("Should return null when no changes occurred", changedReader);
    }

    @Test
    public void testDoOpenIfChangedWithLiveIndexChanges() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Verify initial document count
        int initialDocCount = tsdbDirectoryReader.numDocs();
        assertEquals("Should have initial documents from live and closed indexes", 6, initialDocCount); // 2 live + 2 closed1 + 2 closed2 =
                                                                                                        // 6

        // Search for documents initially
        IndexSearcher initialSearcher = new IndexSearcher(tsdbDirectoryReader);
        TopDocs initialResults = initialSearcher.search(new MatchAllDocsQuery(), 10);
        assertEquals("Initial search should find all documents", initialDocCount, initialResults.totalHits.value());

        // Add a new document to the live index using the existing liveWriter
        liveWriter.addDocument(getLiveDoc("service=newservice,env=test", 1003L, 3000000L, 3999999L));

        // No need to liveWriter.commit(); since DirectoryReader for liveReader should be an NRT reader that reflects live changes

        // Test when live index changes (new document added)
        DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);

        assertNotNull("Should return new reader when live index changes", changedReader);
        assertTrue("Changed reader should be instance of TSDBDirectoryReader", changedReader instanceof TSDBDirectoryReader);

        // Verify the new reader sees the additional document
        TSDBDirectoryReader newMetricsReader = (TSDBDirectoryReader) changedReader;
        int newDocCount = newMetricsReader.numDocs();
        assertEquals("New reader should have one additional document", initialDocCount + 1, newDocCount);

        // Search with the new reader should find the additional document
        IndexSearcher newSearcher = new IndexSearcher(newMetricsReader);
        TopDocs newResults = newSearcher.search(new MatchAllDocsQuery(), 10);
        assertEquals("New search should find all documents including the new one", newDocCount, newResults.totalHits.value());
        assertEquals(
            "Should find one more document than initial search",
            initialResults.totalHits.value() + 1,
            newResults.totalHits.value()
        );

        // Clean up the changed reader
        if (changedReader != null && changedReader != tsdbDirectoryReader) {
            changedReader.close();
        }
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testDoOpenIfChangedWithIndexCommitThrowsException() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1), memChunkReader);
        tsdbDirectoryReader.doOpenIfChanged(null); // IndexCommit parameter
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testDoOpenIfChangedWithIndexWriterThrowsException() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1), memChunkReader);
        tsdbDirectoryReader.doOpenIfChanged(null, false); // IndexWriter parameter
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIndexCommitThrowsException() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1), memChunkReader);
        tsdbDirectoryReader.getIndexCommit();
    }

    @Test
    public void testGetReaderCacheHelperReturnsNull() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        assertNull("CacheHelper should return null", tsdbDirectoryReader.getReaderCacheHelper());
    }

    @Test
    public void testMultipleCloseCalls() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Close multiple times should not throw exceptions
        tsdbDirectoryReader.close();
        tsdbDirectoryReader.close(); // Should be safe to call multiple times

        // Verify underlying readers are only decremented once
        assertEquals("Live reader should only be decremented once", 1, liveReader.getRefCount());
        assertEquals("First closed reader should only be decremented once", 1, closedReader1.getRefCount());
        assertEquals("Second closed reader should only be decremented once", 1, closedReader2.getRefCount());
        assertEquals("Third closed reader should only be decremented once", 1, closedReader3.getRefCount());
    }

    @Test
    public void testWithEmptyReaders() throws IOException {
        // Create empty directories
        Directory emptyLiveDirectory = new ByteBuffersDirectory();
        Directory emptyClosedDirectory1 = new ByteBuffersDirectory();
        Directory emptyClosedDirectory2 = new ByteBuffersDirectory();
        Directory emptyClosedDirectory3 = new ByteBuffersDirectory();

        // Create empty indices
        IndexWriter emptyLiveWriter = new IndexWriter(emptyLiveDirectory, new IndexWriterConfig(new WhitespaceAnalyzer()));
        emptyLiveWriter.commit();
        emptyLiveWriter.close();

        IndexWriter emptyClosedWriter1 = new IndexWriter(emptyClosedDirectory1, new IndexWriterConfig(new WhitespaceAnalyzer()));
        emptyClosedWriter1.commit();
        emptyClosedWriter1.close();

        IndexWriter emptyClosedWriter2 = new IndexWriter(emptyClosedDirectory2, new IndexWriterConfig(new WhitespaceAnalyzer()));
        emptyClosedWriter2.commit();
        emptyClosedWriter2.close();

        IndexWriter emptyClosedWriter3 = new IndexWriter(emptyClosedDirectory3, new IndexWriterConfig(new WhitespaceAnalyzer()));
        emptyClosedWriter3.commit();
        emptyClosedWriter3.close();

        DirectoryReader emptyLiveReader = DirectoryReader.open(emptyLiveDirectory);
        DirectoryReader emptyClosedReader1 = DirectoryReader.open(emptyClosedDirectory1);
        DirectoryReader emptyClosedReader2 = DirectoryReader.open(emptyClosedDirectory2);
        DirectoryReader emptyClosedReader3 = DirectoryReader.open(emptyClosedDirectory3);

        try {
            TSDBDirectoryReader emptyMetricsReader = new TSDBDirectoryReader(
                emptyLiveReader,
                Arrays.asList(emptyClosedReader1, emptyClosedReader2, emptyClosedReader3),
                memChunkReader
            );

            assertEquals("Empty readers should have 0 documents", 0, emptyMetricsReader.numDocs());
            assertEquals("Empty readers should have 0 max docs", 0, emptyMetricsReader.maxDoc());
            assertTrue("Empty readers should be current", emptyMetricsReader.isCurrent());
            assertEquals("Should have 3 closed chunk readers", 3, emptyMetricsReader.getClosedChunkReadersCount());

            emptyMetricsReader.close();
        } finally {
            emptyLiveReader.close();
            emptyClosedReader1.close();
            emptyClosedReader2.close();
            emptyClosedReader3.close();
            emptyLiveDirectory.close();
            emptyClosedDirectory1.close();
            emptyClosedDirectory2.close();
            emptyClosedDirectory3.close();
        }
    }

    @Test
    public void testConcurrentAccess() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Test that multiple incRef/decRef operations work correctly
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                try {
                    tsdbDirectoryReader.incRef();
                    // Do some work
                    Thread.sleep(10);
                    tsdbDirectoryReader.decRef();
                } catch (Exception e) {
                    fail("Concurrent access should not throw exceptions: " + e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Thread interrupted: " + e.getMessage());
            }
        }

        // Reference count should be back to 1
        assertEquals("Reference count should be back to 1 after all threads complete", 1, tsdbDirectoryReader.getRefCount());

        // Note: Underlying readers' reference counts are independent and managed by TSDBDirectoryReader
        // They should have been incremented by 1 during construction
        // These assertions verify the underlying readers still have their incremented counts
        assertTrue("Live reader should have positive reference count", liveReader.getRefCount() > 0);
        assertTrue("Closed reader 1 should have positive reference count", closedReader1.getRefCount() > 0);
        assertTrue("Closed reader 2 should have positive reference count", closedReader2.getRefCount() > 0);
    }

    @Test
    public void testDirectory() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // The directory should be a CompositeDirectory
        assertNotNull("Directory should not be null", tsdbDirectoryReader.directory());
        assertTrue("Directory should be CompositeDirectory instance", tsdbDirectoryReader.directory() instanceof CompositeDirectory);
    }

    @Test
    public void testTryIncRefAfterClose() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);
        tsdbDirectoryReader.close();

        // tryIncRef should return false for closed reader
        assertFalse("tryIncRef should return false for closed reader", tsdbDirectoryReader.tryIncRef());
    }

    @Test
    public void testMatchAllDocsQueryWithThreeClosedReaders() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Create an IndexSearcher using the TSDBDirectoryReader
        IndexSearcher searcher = new IndexSearcher(tsdbDirectoryReader);

        // Execute a MatchAllDocsQuery to get all documents
        MatchAllDocsQuery matchAllQuery = new MatchAllDocsQuery();
        TopDocs topDocs = searcher.search(matchAllQuery, 15);

        // Verify the total number of hits
        // Expected: 2 live documents + 2 closed documents (first reader) + 2 closed documents (second reader) + 3 closed documents (third
        // reader) = 9 total
        int expectedHits = liveReader.numDocs() + closedReader1.numDocs() + closedReader2.numDocs() + closedReader3.numDocs();
        assertEquals("MatchAllDocsQuery should return all documents from all readers", expectedHits, topDocs.totalHits.value());
        assertEquals("Should have 9 total documents", 9, topDocs.totalHits.value());

        // Verify that we got the correct number of score docs returned
        assertEquals(
            "ScoreDocs array should contain all hits when limit >= total hits",
            Math.min(expectedHits, 15),
            topDocs.scoreDocs.length
        );
    }

    @Test
    public void testGetClosedChunkReadersCountWithThreeReaders() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );
        assertEquals("Should have 3 closed chunk readers", 3, tsdbDirectoryReader.getClosedChunkReadersCount());
    }

    @Test
    public void testDuplicateChunkRangeQuery() throws IOException {
        // Test the duplicate chunk scenario where both live and closed indexes contain chunks for the same time range
        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(closedReader1, closedReader2, closedReader3),
            memChunkReader
        );

        // Create an IndexSearcher using the TSDBDirectoryReader
        IndexSearcher searcher = new IndexSearcher(tsdbDirectoryReader);

        // Test 2: Combined query with label filter "service=db,env=prod" AND time range 2000000L-2999999L
        // This should find exactly 2 documents that match both the label and the time range
        BooleanQuery combinedQuery = new BooleanQuery.Builder().add(
            new TermQuery(new Term(Constants.IndexSchema.LABELS, "service=db,env=prod")),
            BooleanClause.Occur.FILTER
        )
            .add(LongPoint.newRangeQuery(Constants.IndexSchema.MIN_TIMESTAMP, Long.MIN_VALUE, 2000000L), BooleanClause.Occur.FILTER)
            .add(LongPoint.newRangeQuery(Constants.IndexSchema.MAX_TIMESTAMP, 2999999L, Long.MAX_VALUE), BooleanClause.Occur.FILTER)
            .build();

        TopDocs combinedResults = searcher.search(combinedQuery, 10);

        // Should find exactly 2 documents:
        // 1. Live index document with "service=db,env=prod" and time range 2000000L-2999999L
        // 2. Closed index document with "service=db,env=prod" and time range 2000000L-2999999L
        assertEquals("Should find 2 documents matching both label and time criteria", 2, combinedResults.totalHits.value());
        assertEquals("Should return 2 score docs for combined query", 2, combinedResults.scoreDocs.length);

        // TODO :
        // Once we added de-duplication logic in LiveSeriesIndexLeafReader we should test the logic with integration tests

    }

    @Test
    public void testDoCloseWithSuppressedExceptions() throws IOException {
        // Test normal close operation to verify basic exception handling structure
        // This test verifies that the doClose method properly handles multiple readers
        // and that it can complete successfully under normal conditions

        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Verify initial state
        assertEquals("Should have reference count of 1", 1, tsdbDirectoryReader.getRefCount());

        // Close should complete successfully
        tsdbDirectoryReader.close();

        // Verify closed state
        assertEquals("Should have reference count of 0 after close", 0, tsdbDirectoryReader.getRefCount());

        // Verify underlying readers are properly decremented
        assertEquals("Live reader should have original ref count", 1, liveReader.getRefCount());
        assertEquals("Closed reader 1 should have original ref count", 1, closedReader1.getRefCount());
        assertEquals("Closed reader 2 should have original ref count", 1, closedReader2.getRefCount());

        // Multiple close calls should be safe
        tsdbDirectoryReader.close(); // Should not throw exception
    }

    /**
     * Validates that doOpenIfChanged() correctly manages references in the success path.
     *
     * Tests that when the live index changes:
     * - A new TSDBDirectoryReader is created
     * - Reference counts are properly managed for new and reused readers
     * - Closing the new reader doesn't leak references
     *
     */
    @Test
    public void testDoOpenIfChangedSuccessPath() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Record initial refCounts
        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosed1RefCount = closedReader1.getRefCount();
        int initialClosed2RefCount = closedReader2.getRefCount();

        // Add a document to trigger a change
        liveWriter.addDocument(getLiveDoc("service=test,env=dev", 1004L, 4000000L, 4999999L));

        // Open changed reader
        DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);

        assertNotNull("Should return new reader when live index changes", changedReader);
        assertTrue("Changed reader should be TSDBDirectoryReader", changedReader instanceof TSDBDirectoryReader);

        // The new reader should see the additional document
        TSDBDirectoryReader newMetricsReader = (TSDBDirectoryReader) changedReader;
        assertEquals("New reader should see the new document", tsdbDirectoryReader.numDocs() + 1, newMetricsReader.numDocs());

        // Clean up the new reader
        newMetricsReader.close();

        // Verify original readers' refCounts are unchanged
        // The new reader uses new DirectoryReader instances, not the original ones
        assertEquals("Live reader refCount should be unchanged", initialLiveRefCount, liveReader.getRefCount());
        assertEquals("Closed1 reader refCount should match initial", initialClosed1RefCount, closedReader1.getRefCount());
        assertEquals("Closed2 reader refCount should match initial", initialClosed2RefCount, closedReader2.getRefCount());
    }

    /**
     * DirectoryReader wrapper that can be configured to throw IOException during refresh.
     * Used to test error handling and cleanup logic in TSDBDirectoryReader.doOpenIfChanged().
     */
    private static class ThrowingOnRefreshDirectoryReader extends DirectoryReader {
        private final DirectoryReader delegate;
        private boolean shouldThrow = false;

        ThrowingOnRefreshDirectoryReader(DirectoryReader delegate) throws IOException {
            super(delegate.directory(), delegate.leaves().stream().map(ctx -> ctx.reader()).toArray(LeafReader[]::new), null);
            this.delegate = delegate;
            delegate.incRef();
        }

        void enableThrowOnRefresh() {
            this.shouldThrow = true;
        }

        @Override
        protected DirectoryReader doOpenIfChanged() throws IOException {
            if (shouldThrow) {
                throw new IOException("Simulated failure during openIfChanged");
            }
            DirectoryReader changed = DirectoryReader.openIfChanged(delegate);
            if (changed != null) {
                return new ThrowingOnRefreshDirectoryReader(changed);
            }
            return null;
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
            throw new UnsupportedOperationException("Not supported in test helper");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
            throw new UnsupportedOperationException("Not supported in test helper");
        }

        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public boolean isCurrent() throws IOException {
            return delegate.isCurrent();
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            return delegate.getIndexCommit();
        }

        @Override
        protected void doClose() throws IOException {
            delegate.decRef();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    /**
     * Tests error handling and cleanup in doOpenIfChanged when an exception occurs during refresh.
     * Verifies that newly created readers are properly decRef'd while reused readers are not,
     * preventing both resource leaks and double-decRef errors.
     */
    @Test
    public void testDoOpenIfChangedErrorHandlingWithThrowingReader() throws IOException {
        ThrowingOnRefreshDirectoryReader throwingClosedChunkReader1 = new ThrowingOnRefreshDirectoryReader(closedReader1);
        ThrowingOnRefreshDirectoryReader throwingClosedChunkReader2 = new ThrowingOnRefreshDirectoryReader(closedReader2);

        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(throwingClosedChunkReader1, throwingClosedChunkReader2),
            memChunkReader
        );

        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosedChunkReader1RefCount = throwingClosedChunkReader1.getRefCount();
        int initialClosedChunkReader2RefCount = throwingClosedChunkReader2.getRefCount();

        liveWriter.addDocument(getLiveDoc("service=test,env=dev", 1004L, 4000000L, 4999999L));
        liveWriter.commit();

        throwingClosedChunkReader2.enableThrowOnRefresh();

        try {
            DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);
            if (changedReader != null) {
                changedReader.close();
            }
            fail("Expected IOException during refresh");
        } catch (IOException e) {
            assertTrue("Should have simulated failure message", e.getMessage().contains("Simulated failure"));
        }

        assertEquals("New live reader should be cleaned up", initialLiveRefCount, liveReader.getRefCount());
        assertEquals(
            "Reused closed chunk reader should not be decRef'd",
            initialClosedChunkReader1RefCount,
            throwingClosedChunkReader1.getRefCount()
        );
        assertEquals(
            "Throwing closed chunk reader should not be decRef'd",
            initialClosedChunkReader2RefCount,
            throwingClosedChunkReader2.getRefCount()
        );
        assertEquals("Original reader should be usable", 1, tsdbDirectoryReader.getRefCount());
        assertTrue("Original reader should have docs", tsdbDirectoryReader.numDocs() > 0);

        throwingClosedChunkReader1.close();
        throwingClosedChunkReader2.close();
    }

    /**
     * Verifies that doOpenIfChanged properly manages reference counts in the success path.
     *
     * <p>This test validates that newly created readers are properly decRef'd after being passed to
     * the TSDBDirectoryReader constructor, ensuring only the MDR owns them and preventing reference leaks.
     *
     * <p>Expected reference counting flow:
     * <ol>
     *   <li>openIfChanged() returns new reader with refCount=1</li>
     *   <li>TSDBDirectoryReader constructor incRef's to refCount=2</li>
     *   <li>doOpenIfChanged success path decRef's back to refCount=1</li>
     *   <li>Only TSDBDirectoryReader owns the new readers</li>
     * </ol>
     *
     * @throws IOException if an error occurs during reader operations
     */
    @Test
    public void testDoOpenIfChangedProperlyDecRefsNewReaders() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Capture initial reference counts before refresh
        DirectoryReader initialLiveReader = liveReader;
        int initialLiveReaderRefCount = initialLiveReader.getRefCount();
        int initialClosed1RefCount = closedReader1.getRefCount();
        int initialClosed2RefCount = closedReader2.getRefCount();

        // Modify live index to trigger refresh
        liveWriter.addDocument(getLiveDoc("service=newservice,env=prod", 2001L, 8000000L, 8999999L));
        liveWriter.commit();

        // Refresh creates new live reader but reuses closed readers
        DirectoryReader changedMetricsReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);

        assertNotNull("Changed reader should not be null after live index changes", changedMetricsReader);
        assertTrue("Changed reader should be TSDBDirectoryReader", changedMetricsReader instanceof TSDBDirectoryReader);
        assertEquals("New reader should have one more document", tsdbDirectoryReader.numDocs() + 1, changedMetricsReader.numDocs());

        // Core assertion: Verify new live reader has refCount=1 (owned only by new MDR)
        try {
            java.lang.reflect.Field liveReaderField = TSDBDirectoryReader.class.getDeclaredField("liveSeriesIndexDirectoryReader");
            liveReaderField.setAccessible(true);
            DirectoryReader newLiveReader = (DirectoryReader) liveReaderField.get(changedMetricsReader);

            assertNotNull("New live reader should not be null", newLiveReader);
            assertNotSame("New live reader should be different from old", initialLiveReader, newLiveReader);
            assertEquals("New live reader refCount should be 1 (owned only by new MDR)", 1, newLiveReader.getRefCount());
        } catch (Exception e) {
            fail("Failed to access liveSeriesIndexDirectoryReader via reflection: " + e.getMessage());
        }

        // Verify reference counts after refresh
        assertEquals(
            "Original live reader refCount unchanged after new reader created",
            initialLiveReaderRefCount,
            initialLiveReader.getRefCount()
        );
        assertEquals("Closed reader 1 refCount incremented (reused by both MDRs)", initialClosed1RefCount + 1, closedReader1.getRefCount());
        assertEquals("Closed reader 2 refCount incremented (reused by both MDRs)", initialClosed2RefCount + 1, closedReader2.getRefCount());

        // Close old MDR and verify reference counts
        tsdbDirectoryReader.close();

        assertEquals(
            "Original live reader refCount decremented after old MDR closed",
            initialLiveReaderRefCount - 1,
            initialLiveReader.getRefCount()
        );
        assertEquals("Closed reader 1 refCount back to initial after old MDR closed", initialClosed1RefCount, closedReader1.getRefCount());
        assertEquals("Closed reader 2 refCount back to initial after old MDR closed", initialClosed2RefCount, closedReader2.getRefCount());

        // Close new MDR and verify final cleanup
        changedMetricsReader.close();

        assertEquals(
            "Closed reader 1 refCount back to initial - 1 after both MDRs closed",
            initialClosed1RefCount - 1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Closed reader 2 refCount back to initial - 1 after both MDRs closed",
            initialClosed2RefCount - 1,
            closedReader2.getRefCount()
        );

        tsdbDirectoryReader = null;
    }

    /**
     * Tests that new closed chunk readers are properly decRef'd in the success path.
     * Mirrors testDoOpenIfChangedProperlyDecRefsNewReaders but for closed chunk readers instead of live readers.
     */
    @Test
    public void testDoOpenIfChangedProperlyDecRefsNewClosedChunkReaders() throws IOException {
        tsdbDirectoryReader = new TSDBDirectoryReader(liveReader, Arrays.asList(closedReader1, closedReader2), memChunkReader);

        // Capture initial reference counts before refresh
        DirectoryReader initialClosedReader1 = closedReader1;
        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosed1RefCount = initialClosedReader1.getRefCount();
        int initialClosed2RefCount = closedReader2.getRefCount();

        // Modify closed chunk index 1 to trigger refresh
        IndexWriter closedWriter1 = new IndexWriter(closedReader1.directory(), new IndexWriterConfig(new WhitespaceAnalyzer()));
        closedWriter1.addDocument(getClosedDoc("service=newservice,env=test", 1500000L, 1599999L));
        closedWriter1.commit();
        closedWriter1.close();

        // Refresh creates new closed chunk reader 1 but reuses live reader and closed reader 2
        DirectoryReader changedMetricsReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);

        assertNotNull("Changed reader should not be null after closed chunk index changes", changedMetricsReader);
        assertTrue("Changed reader should be TSDBDirectoryReader", changedMetricsReader instanceof TSDBDirectoryReader);
        assertEquals("New reader should have one more document", tsdbDirectoryReader.numDocs() + 1, changedMetricsReader.numDocs());

        // Core assertion: Verify new closed chunk reader 1 has refCount=1 (owned only by new MDR)
        try {
            java.lang.reflect.Field closedReadersField = TSDBDirectoryReader.class.getDeclaredField("closedChunkIndexDirectoryReaders");
            closedReadersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<DirectoryReader> newClosedReaders = (List<DirectoryReader>) closedReadersField.get(changedMetricsReader);

            assertNotNull("New closed readers list should not be null", newClosedReaders);
            assertEquals("Should have 2 closed chunk readers", 2, newClosedReaders.size());

            DirectoryReader newClosedReader1 = newClosedReaders.get(0);
            assertNotNull("New closed reader 1 should not be null", newClosedReader1);
            assertNotSame("New closed reader 1 should be different from old", initialClosedReader1, newClosedReader1);
            assertEquals("New closed reader 1 refCount should be 1 (owned only by new MDR)", 1, newClosedReader1.getRefCount());
        } catch (Exception e) {
            fail("Failed to access closedChunkIndexDirectoryReaders via reflection: " + e.getMessage());
        }

        // Verify reference counts after refresh
        assertEquals(
            "Original closed reader 1 refCount unchanged after new reader created",
            initialClosed1RefCount,
            initialClosedReader1.getRefCount()
        );
        assertEquals("Live reader refCount incremented (reused by both MDRs)", initialLiveRefCount + 1, liveReader.getRefCount());
        assertEquals("Closed reader 2 refCount incremented (reused by both MDRs)", initialClosed2RefCount + 1, closedReader2.getRefCount());

        // Close old MDR and verify reference counts
        tsdbDirectoryReader.close();

        assertEquals(
            "Original closed reader 1 refCount decremented after old MDR closed",
            initialClosed1RefCount - 1,
            initialClosedReader1.getRefCount()
        );
        assertEquals("Live reader refCount back to initial after old MDR closed", initialLiveRefCount, liveReader.getRefCount());
        assertEquals("Closed reader 2 refCount back to initial after old MDR closed", initialClosed2RefCount, closedReader2.getRefCount());

        // Close new MDR and verify final cleanup
        changedMetricsReader.close();

        assertEquals("Live reader refCount back to initial - 1 after both MDRs closed", initialLiveRefCount - 1, liveReader.getRefCount());
        assertEquals(
            "Closed reader 2 refCount back to initial - 1 after both MDRs closed",
            initialClosed2RefCount - 1,
            closedReader2.getRefCount()
        );

        tsdbDirectoryReader = null;
    }

    /**
     * Tests error handling when live reader hasn't changed (newLiveSeriesReader == null)
     * but a closed chunk reader throws during refresh. Verifies the null branch of cleanup logic.
     */
    @Test
    public void testDoOpenIfChangedErrorHandlingWhenLiveReaderUnchanged() throws IOException {
        ThrowingOnRefreshDirectoryReader throwingClosedChunkReader1 = new ThrowingOnRefreshDirectoryReader(closedReader1);
        ThrowingOnRefreshDirectoryReader throwingClosedChunkReader2 = new ThrowingOnRefreshDirectoryReader(closedReader2);

        tsdbDirectoryReader = new TSDBDirectoryReader(
            liveReader,
            Arrays.asList(throwingClosedChunkReader1, throwingClosedChunkReader2),
            memChunkReader
        );

        int initialLiveRefCount = liveReader.getRefCount();
        int initialClosedChunkReader1RefCount = throwingClosedChunkReader1.getRefCount();
        int initialClosedChunkReader2RefCount = throwingClosedChunkReader2.getRefCount();

        throwingClosedChunkReader2.enableThrowOnRefresh();

        try {
            DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);
            if (changedReader != null) {
                changedReader.close();
            }
            fail("Expected IOException during refresh");
        } catch (IOException e) {
            assertTrue("Should have simulated failure", e.getMessage().contains("Simulated failure"));
        }

        assertEquals("Live reader should be unchanged (was not refreshed)", initialLiveRefCount, liveReader.getRefCount());
        assertEquals(
            "Closed chunk reader 1 should be unchanged",
            initialClosedChunkReader1RefCount,
            throwingClosedChunkReader1.getRefCount()
        );
        assertEquals(
            "Closed chunk reader 2 should be unchanged",
            initialClosedChunkReader2RefCount,
            throwingClosedChunkReader2.getRefCount()
        );

        throwingClosedChunkReader1.close();
        throwingClosedChunkReader2.close();
    }

    /**
     * DirectoryReader that throws IOException during close to test suppressed exception handling.
     * When cleanup code calls decRef() on a newly created reader, it triggers doClose() which can throw.
     */
    private static class ThrowingOnCloseDirectoryReader extends DirectoryReader {
        private final DirectoryReader delegate;
        private boolean shouldThrowOnClose = false;
        private boolean shouldThrowOnRefresh = false;

        ThrowingOnCloseDirectoryReader(DirectoryReader delegate) throws IOException {
            super(delegate.directory(), delegate.leaves().stream().map(ctx -> ctx.reader()).toArray(LeafReader[]::new), null);
            this.delegate = delegate;
            delegate.incRef();
        }

        void enableThrowOnClose() {
            this.shouldThrowOnClose = true;
        }

        void disableThrowOnClose() {
            this.shouldThrowOnClose = false;
        }

        void enableThrowOnRefresh() {
            this.shouldThrowOnRefresh = true;
        }

        @Override
        protected DirectoryReader doOpenIfChanged() throws IOException {
            if (shouldThrowOnRefresh) {
                throw new IOException("Simulated failure during openIfChanged");
            }
            DirectoryReader changed = DirectoryReader.openIfChanged(delegate);
            if (changed != null) {
                ThrowingOnCloseDirectoryReader newReader = new ThrowingOnCloseDirectoryReader(changed);
                // If this reader is configured to throw on close, propagate that to the new reader
                if (shouldThrowOnClose) {
                    newReader.enableThrowOnClose();
                }
                return newReader;
            }
            return null;
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
            throw new UnsupportedOperationException("Not supported in test helper");
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
            throw new UnsupportedOperationException("Not supported in test helper");
        }

        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public boolean isCurrent() throws IOException {
            return delegate.isCurrent();
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            return delegate.getIndexCommit();
        }

        @Override
        protected void doClose() throws IOException {
            try {
                delegate.decRef();
            } finally {
                if (shouldThrowOnClose) {
                    throw new IOException("Simulated failure during decRef cleanup");
                }
            }
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    /**
     * Tests suppressed exception handling when decRef throws during cleanup of both live and closed chunk readers.
     * Verifies that exceptions from decRef are properly suppressed and don't mask the original error.
     *
     */
    @Test
    public void testDoOpenIfChangedSuppressedExceptionHandling() throws IOException {
        ThrowingOnCloseDirectoryReader throwingLiveReader = new ThrowingOnCloseDirectoryReader(liveReader);
        ThrowingOnCloseDirectoryReader throwingClosedChunkReader1 = new ThrowingOnCloseDirectoryReader(closedReader1);
        ThrowingOnRefreshDirectoryReader throwingClosedChunkReader2 = new ThrowingOnRefreshDirectoryReader(closedReader2);

        tsdbDirectoryReader = new TSDBDirectoryReader(
            throwingLiveReader,
            Arrays.asList(throwingClosedChunkReader1, throwingClosedChunkReader2),
            memChunkReader
        );

        liveWriter.addDocument(getLiveDoc("service=test,env=dev", 1004L, 4000000L, 4999999L));
        liveWriter.commit();

        IndexWriter writer1 = new IndexWriter(closedReader1.directory(), new IndexWriterConfig(new WhitespaceAnalyzer()));
        writer1.addDocument(getClosedDoc("service=test", 100L, 200L));
        writer1.commit();
        writer1.close();

        throwingLiveReader.enableThrowOnClose();
        throwingClosedChunkReader1.enableThrowOnClose();
        throwingClosedChunkReader2.enableThrowOnRefresh();

        try {
            DirectoryReader changedReader = DirectoryReader.openIfChanged(tsdbDirectoryReader);
            if (changedReader != null) {
                changedReader.close();
            }
            fail("Expected IOException during refresh");
        } catch (IOException e) {
            assertTrue("Primary exception should be from openIfChanged", e.getMessage().contains("Simulated failure"));

            Throwable[] suppressed = e.getSuppressed();
            assertTrue("Should have at least 2 suppressed exceptions (live + closed chunk reader)", suppressed.length >= 2);

            int cleanupExceptionCount = 0;
            for (Throwable t : suppressed) {
                if (t.getMessage() != null && t.getMessage().contains("Simulated failure during decRef cleanup")) {
                    cleanupExceptionCount++;
                }
            }
            assertEquals("Should have suppressed exceptions from both live and closed chunk reader cleanup", 2, cleanupExceptionCount);
        }

        throwingLiveReader.disableThrowOnClose();
        throwingClosedChunkReader1.disableThrowOnClose();
        throwingLiveReader.close();
        throwingClosedChunkReader1.close();
        throwingClosedChunkReader2.close();
    }

    private Document getLiveDoc(String labels, long reference, long mint, long maxt) {
        Document doc = new Document();
        doc.add(new StringField(Constants.IndexSchema.LABELS, labels, Field.Store.NO));
        doc.add(new LongPoint(Constants.IndexSchema.REFERENCE, reference));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, reference));
        doc.add(new LongPoint(Constants.IndexSchema.MIN_TIMESTAMP, mint));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, mint));
        doc.add(new LongPoint(Constants.IndexSchema.MAX_TIMESTAMP, maxt));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MAX_TIMESTAMP, maxt));
        return doc;
    }

    private Map<Long, List<MemChunk>> getMemChunksForLiveIndex() {
        return Map.of(
            1001L,
            List.of(getMockChunk(1000000L, 1999999L)),
            1002L,
            List.of(getMockChunk(2000000L, 2999999L)),
            1003L,
            List.of(getMockChunk(3000000L, 3999999L))
        );

    }

    private Document getClosedDoc(String labels, long mint, long maxt) {
        Document doc = new Document();
        MemChunk memChunk = getMockChunk(mint, maxt);

        doc.add(new StringField(Constants.IndexSchema.LABELS, labels, Field.Store.NO));
        doc.add(
            new BinaryDocValuesField(Constants.IndexSchema.CHUNK, ClosedChunkIndexIO.serializeChunk(memChunk.getCompoundChunk().toChunk()))
        );
        doc.add(new LongPoint(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new LongPoint(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        return doc;
    }

    private MemChunk getMockChunk(long mint, long maxt) {
        MemChunk memChunk = new MemChunk(1, mint, maxt, null, Encoding.XOR);

        for (long i = 0; i < 10; i++) {
            double value = i * 1.5;
            memChunk.append(i, value, 0L);
        }
        return memChunk;
    }
}
