# Serialization & Deserialization Testing Guide

## Scope

**This guide is for:** Classes with `toXContent()` / `fromArgs()` / `parse()` methods

**Common examples:**
- Pipeline stages (`SubtractStage`, `ValueFilterStage`, etc.)
- Aggregation builders (`TimeSeriesCoordinatorAggregationBuilder`, etc.)
- Any class that serializes to/from JSON

**When to use:** Writing tests for classes that serialize to JSON or parse JSON back

---

## Critical Rule: Always Test Round-Trips

**The Problem:** A test that only validates JSON output can miss bugs:

```java
// ❌ BAD: Only tests that JSON is produced
public void testToXContent() throws IOException {
    ValueFilterStage stage = new ValueFilterStage(ValueFilterType.GE, 10.5);
    XContentBuilder builder = XContentFactory.jsonBuilder();

    builder.startObject();
    stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
    builder.endObject();

    String json = builder.toString();
    assertTrue(json.contains("operator"));  // Passes even if value is wrong!
    assertTrue(json.contains("ge"));
}
```

**Why it's bad:**
- Only checks that *some* value exists
- Doesn't verify the JSON can be **parsed back**
- Misses serialization/deserialization mismatches

---

## Pattern 1: Round-Trip Testing

**The Solution:** Serialize → Parse → Deserialize → Verify

```java
// ✅ GOOD: Tests full round-trip
public void testToXContentRoundTrip() throws IOException {
    // Test all operator types to ensure round-trip works for each
    ValueFilterType[] operators = ValueFilterType.values();

    for (ValueFilterType operator : operators) {
        // 1. Create original object
        ValueFilterStage original = new ValueFilterStage(operator, 42.5);

        // 2. Serialize to JSON
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        // 3. Parse the JSON back to a map
        String json = builder.toString();
        Map<String, Object> args;
        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            args = parser.map();
        }

        // 4. Recreate the object from parsed args
        ValueFilterStage deserialized = ValueFilterStage.fromArgs(args);

        // 5. Verify they're equal
        assertEquals(
            "Round-trip failed for operator " + operator,
            original.getOperator(),
            deserialized.getOperator()
        );
        assertEquals(
            original.getTargetValue(),
            deserialized.getTargetValue(),
            0.001
        );
        assertEquals(original, deserialized);
    }
}
```

**Key Benefits:**
- Catches serialization/deserialization mismatches
- Tests all variants (all enum values, different parameters, etc.)
- Ensures JSON can actually be parsed back
- Validates `equals()` implementation

**Location:** `ValueFilterStageTests.java:227-269`

---

## Pattern 2: XContent Parser Testing

For classes with `parse()` methods (like aggregation builders):

```java
public void testParseStagesWithBooleanValues() throws Exception {
    String json = """
        {
          "stages": [
            {
              "type": "subtract",
              "keep_nans": false,          // Boolean parameter
              "right_op_reference": "4"
            },
            {
              "type": "value_filter",
              "operator": "ge",
              "target_value": 200000000    // Number parameter
            }
          ],
          "references": {
            "0": "0_coordinator",
            "4": "4_coordinator"
          },
          "inputReference": "0"            // String parameter
        }
        """;

    try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
        parser.nextToken(); // Move to START_OBJECT

        TimeSeriesCoordinatorAggregationBuilder result =
            TimeSeriesCoordinatorAggregationBuilder.parse("coord_with_boolean", parser);

        // Verify parsing succeeded
        assertEquals("coord_with_boolean", result.getName());
        assertEquals(2, result.getStages().size());
        assertEquals(2, result.getReferences().size());

        // Verify the parsed builder can create an aggregator (validation passes)
        assertNotNull(result.createInternal(Map.of()));
    }
}
```

**Location:** `TimeSeriesCoordinatorAggregationBuilderTests.java:409-444`

### Testing All Token Types

When writing `parse()` methods, ensure you handle **all XContent token types**:

```java
// In your parser implementation:
if (token == XContentParser.Token.VALUE_STRING) {
    stageArgs.put(fieldName, parser.text());
} else if (token == XContentParser.Token.VALUE_BOOLEAN) {  // ✅ Don't forget!
    stageArgs.put(fieldName, parser.booleanValue());
} else if (token == XContentParser.Token.VALUE_NUMBER) {
    if (parser.numberType() == XContentParser.NumberType.INT) {
        stageArgs.put(fieldName, parser.intValue());
    } else {
        stageArgs.put(fieldName, parser.doubleValue());
    }
} else if (token == XContentParser.Token.START_ARRAY) {
    List<String> arrayValues = new ArrayList<>();
    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
        if (token == XContentParser.Token.VALUE_STRING) {
            arrayValues.add(parser.text());
        }
    }
    stageArgs.put(fieldName, arrayValues);
}
```

**Test each token type:**
```java
public void testParseWithAllTokenTypes() {
    String json = """
        {
          "string_field": "value",
          "boolean_field": true,
          "number_field": 42,
          "double_field": 3.14,
          "array_field": ["a", "b", "c"]
        }
        """;
    // ... parse and verify all fields
}
```

---

## Pattern 3: Real-World Complex Examples

Always include at least one test with **real production data**:

```java
/**
 * Test parsing a real-world complex aggregation configuration.
 * This uses actual data from production queries to ensure parser handles
 * complex nested structures, boolean values, and various stage types.
 */
public void testParseRealWorldComplexAggregation() throws Exception {
    String json = """
        {
          "buckets_path": [],
          "stages": [
            {
              "type": "subtract",
              "keep_nans": false,
              "right_op_reference": "4"
            },
            {
              "type": "value_filter",
              "operator": "ge",
              "target_value": 200000000
            },
            {
              "type": "remove_empty"
            },
            {
              "type": "alias_by_tags",
              "tag_names": ["namespace"]
            }
          ],
          "references": {
            "0": "0_coordinator",
            "4": "4_coordinator"
          },
          "inputReference": "0"
        }
        """;

    try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
        parser.nextToken();

        TimeSeriesCoordinatorAggregationBuilder result =
            TimeSeriesCoordinatorAggregationBuilder.parse("9", parser);

        // Verify complex structure was parsed correctly
        assertEquals("9", result.getName());
        assertEquals(4, result.getStages().size());
        assertEquals(2, result.getReferences().size());

        // Most important: verify it can be used
        assertNotNull(result.createInternal(Map.of()));
    }
}
```

**Location:** `TimeSeriesCoordinatorAggregationBuilderTests.java:454-505`

---

## Common Pitfalls & Solutions

### Pitfall 1: Enum Serialization Mismatch

**Problem:** Using `enum.name()` produces uppercase, but parser expects lowercase

```java
// ❌ BAD: Serializes "GE" but parser expects "ge"
public void toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field("operator", operator.name()); // Returns "GE"
}

public static ValueFilterType fromString(String name) {
    return switch (name) {
        case "ge", ">=", "removeBelowValue" -> ValueFilterType.GE;  // Only lowercase!
        // ...
    };
}
```

**Solution 1:** Use lowercase in serialization
```java
// ✅ GOOD: Match what parser expects
public void toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field("operator", operator.name().toLowerCase());
}
```

**Solution 2:** Add a canonical string method to enum
```java
public enum ValueFilterType {
    GE, GT, LE, LT, EQ, NE;

    public String toStringValue() {
        return name().toLowerCase();
    }
}

// In toXContent:
builder.field("operator", operator.toStringValue());
```

**Detection:** Round-trip tests will catch this immediately!

**Reference:** `ValueFilterType.java:52-66` and `ValueFilterStage.java:146-149`

---

### Pitfall 2: Missing Boolean Token Handling

**Problem:** Parser doesn't handle `VALUE_BOOLEAN` tokens

```java
// ❌ BAD: Throws exception when encountering boolean
if (token == XContentParser.Token.VALUE_STRING) {
    stageArgs.put(fieldName, parser.text());
} else if (token == XContentParser.Token.VALUE_NUMBER) {
    stageArgs.put(fieldName, parser.doubleValue());
}
// No boolean handling! ❌
```

**Error:**
```
Unsupported token type for stage argument 'keep_nans': VALUE_BOOLEAN
```

**Solution:**
```java
// ✅ GOOD: Handle all value types
if (token == XContentParser.Token.VALUE_STRING) {
    stageArgs.put(fieldName, parser.text());
} else if (token == XContentParser.Token.VALUE_BOOLEAN) {
    stageArgs.put(fieldName, parser.booleanValue());  // ✅ Added!
} else if (token == XContentParser.Token.VALUE_NUMBER) {
    // ...
}
```

**Location:** `TimeSeriesCoordinatorAggregationBuilder.java:338-339`

---

### Pitfall 3: Case Sensitivity

**Problem:** String matching is case-sensitive

```java
// Parser expects lowercase
ValueFilterType.fromString("GE")  // ❌ Throws: Unknown filter function: GE
ValueFilterType.fromString("ge")  // ✅ Works!
```

**Accepted values for GE operator:**
- `"ge"` (lowercase) ✅
- `">="` (symbol) ✅
- `"removeBelowValue"` (alias) ✅
- `"GE"` (uppercase) ❌ **Does not work!**

**Solution:** Always use lowercase or symbols in JSON

**Reference:** `ValueFilterType.java:52-66` and `Constants.java:213`

---

## Testing Checklist

For any class with serialization/deserialization:

- [ ] **Round-trip test exists**
  - [ ] Tests all variants (enum values, parameter combinations)
  - [ ] Serializes with `toXContent()`
  - [ ] Parses back with `fromArgs()` or `parse()`
  - [ ] Verifies equality with original

- [ ] **XContent parser tests** (if applicable)
  - [ ] Tests string values
  - [ ] Tests boolean values ✅ **Critical!**
  - [ ] Tests number values (int and double)
  - [ ] Tests array values
  - [ ] Tests nested objects

- [ ] **Real-world example test**
  - [ ] Uses actual production query structure
  - [ ] Tests complex nested configurations
  - [ ] Verifies parsed object is usable

- [ ] **Enum serialization** (if applicable)
  - [ ] Uses lowercase, not `.name()`
  - [ ] Round-trip test catches mismatches

- [ ] **Error cases**
  - [ ] Missing required fields
  - [ ] Invalid values
  - [ ] Wrong types

---

## Template: Complete Serialization Test Class

```java
public class YourStageTests extends AbstractWireSerializingTestCase<YourStage> {

    // ============ Standard toXContent Test ============

    public void testToXContent() throws IOException {
        YourStage stage = new YourStage(param1, param2);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("param1"));
        assertTrue(json.contains("param2"));
    }

    // ============ Round-Trip Test (CRITICAL!) ============

    public void testToXContentRoundTrip() throws IOException {
        // Test all variants
        for (Variant variant : getAllVariants()) {
            YourStage original = new YourStage(variant);

            // Serialize
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            // Parse
            String json = builder.toString();
            Map<String, Object> args;
            try (var parser = createParser(JsonXContent.jsonXContent, json)) {
                args = parser.map();
            }

            // Deserialize
            YourStage deserialized = YourStage.fromArgs(args);

            // Verify
            assertEquals("Round-trip failed for " + variant, original, deserialized);
        }
    }

    // ============ FromArgs Tests ============

    public void testFromArgsValid() {
        Map<String, Object> args = Map.of(
            "param1", "value1",
            "param2", true,
            "param3", 42
        );

        YourStage stage = YourStage.fromArgs(args);

        assertNotNull(stage);
        assertEquals("value1", stage.getParam1());
        assertTrue(stage.getParam2());
        assertEquals(42, stage.getParam3());
    }

    public void testFromArgsMissingRequired() {
        Map<String, Object> args = Map.of("param2", true);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> YourStage.fromArgs(args)
        );
        assertTrue(exception.getMessage().contains("requires 'param1'"));
    }

    // ============ Wire Serialization (inherited) ============

    @Override
    protected YourStage createTestInstance() {
        return new YourStage(
            randomAlphaOfLength(5),
            randomBoolean(),
            randomInt(100)
        );
    }

    @Override
    protected Writeable.Reader<YourStage> instanceReader() {
        return YourStage::readFrom;
    }
}
```

---

## Examples in Codebase

### Excellent Examples:
- `ValueFilterStageTests.java:227-269` - Round-trip test for all operators
- `TimeSeriesCoordinatorAggregationBuilderTests.java:409-505` - Boolean handling and real-world examples
- `SubtractStageTests.java` - Comprehensive fromArgs testing

### Reference Implementations:
- `ValueFilterStage.java` - Proper enum serialization (after fix)
- `TimeSeriesCoordinatorAggregationBuilder.java:309-396` - Complete parser with all token types
- `SubtractStage.java:183-188` - fromArgs with boolean parameter

---

## Quick Reference

| What to Test | Example | Why |
|--------------|---------|-----|
| Round-trip | Serialize → Parse → Deserialize → Compare | Catches serialization mismatches |
| All enum values | `for (Type t : Type.values())` | Ensures all variants work |
| Boolean values | `"keep_nans": false` | Often forgotten in parsers |
| Real examples | Production query JSON | Tests realistic usage |
| Error cases | Missing fields, wrong types | Validates error handling |

**Remember:** If you only test JSON output without parsing it back, you haven't tested serialization!
