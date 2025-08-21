/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tsdb.core.model;

import org.opensearch.test.OpenSearchTestCase;

public class ByteLabelsTests extends OpenSearchTestCase {

    public void testBasicFunctionality() {
        ByteLabels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        assertEquals("v1", labels.get("k1"));
        assertEquals("v2", labels.get("k2"));
        assertEquals("", labels.get("nonexistent"));
        assertEquals("", labels.get(null));
        assertEquals("", labels.get(""));
        assertTrue(labels.has("k1"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.has(null));
        assertFalse(labels.has(""));
        assertFalse(labels.isEmpty());
        
        // Test toKeyValueString
        String kvString = labels.toKeyValueString();
        assertTrue("Should contain k1:v1", kvString.contains("k1:v1"));
        assertTrue("Should contain k2:v2", kvString.contains("k2:v2"));
        
        // Test toString
        assertEquals("toString should match toKeyValueString", kvString, labels.toString());
    }

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromStrings("k1", "v1", "k2"));
    }

    public void testEmptyLabels() {
        ByteLabels empty = ByteLabels.emptyLabels();
        assertTrue(empty.isEmpty());
        assertEquals("", empty.toKeyValueString());
        assertEquals("", empty.toString());
        assertEquals("", empty.get("anything"));
        assertFalse(empty.has("anything"));

        ByteLabels emptyFromMap = ByteLabels.fromMap(java.util.Map.of());
        assertTrue(emptyFromMap.isEmpty());
        assertEquals(empty, emptyFromMap);
    }

    public void testLabelSorting() {
        ByteLabels labels1 = ByteLabels.fromStrings("zebra", "z", "apple", "a");
        ByteLabels labels2 = ByteLabels.fromStrings("apple", "a", "zebra", "z");

        assertEquals(labels1.toMapView(), labels2.toMapView());
        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1, labels2);
    }

    public void testStableHash() {
        ByteLabels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        ByteLabels labels2 = ByteLabels.fromStrings("k2", "v2", "k1", "v1");

        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1.hashCode(), labels2.hashCode());
    }

    public void testLongStringEncoding() {
        // Create a string longer than 254 bytes to test extended encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);
        
        ByteLabels labels = ByteLabels.fromStrings(longKey, longValue, "short", "val");
        
        assertEquals(longValue, labels.get(longKey));
        assertEquals("val", labels.get("short"));
        assertTrue(labels.has(longKey));
        
        // Verify it works with fromMap too
        ByteLabels labels2 = ByteLabels.fromMap(java.util.Map.of(longKey, longValue, "short", "val"));
        assertEquals(labels, labels2);
    }

    public void testEqualsAndHashCode() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2");
        ByteLabels labels2 = ByteLabels.fromStrings("b", "2", "a", "1"); // Different order
        ByteLabels labels3 = ByteLabels.fromStrings("a", "1", "b", "3"); // Different value
        
        // Test equals
        assertEquals("Same labels should be equal", labels1, labels2);
        assertNotEquals("Different labels should not be equal", labels1, labels3);
        
        // Test hashCode consistency
        assertEquals("Equal objects should have same hashCode", labels1.hashCode(), labels2.hashCode());
    }

}
