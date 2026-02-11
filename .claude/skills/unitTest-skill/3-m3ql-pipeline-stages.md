# M3QL Pipeline Stages - Unit Testing Guide

## Scope

**This guide is for:** M3QL query pipeline stages (`src/main/java/org/opensearch/tsdb/lang/m3/stage/`)

**Project path:** `tsdb/lang/m3/stage/` and related query pipeline components

**When to use:** Adding new pipeline stages like `excludeByTag`, `perSecond`, `sum`, `moving`, etc.

## Component Architecture

### M3QL Pipeline Stage Testing Layers

Each new pipeline stage requires tests at 4 layers:

```
1. Stage Implementation Tests (ExcludeByTagStageTests)
   └─ Core logic, serialization, factory methods

2. Plan Node Tests (ExcludeByTagPlanNodeTests)
   └─ AST → Plan conversion, visitor pattern

3. Visitor Integration Tests (SourceBuilderVisitorTests)
   └─ Plan → DSL translation, child validation

4. End-to-End Tests (4-file M3QL tests)
   └─ M3QL → AST → Plan → DSL pipeline validation
```

---

## 1. Pipeline Stage Tests

**Location:** `src/test/java/org/opensearch/tsdb/lang/m3/stage/YourStageTests.java`

**Pattern:** Extend `AbstractWireSerializingTestCase<YourStage>`

**Example:** `ExcludeByTagStageTests.java`

### Required Test Categories

#### Core Functionality Tests (10-15 tests)
- Basic operation with valid inputs
- Multiple parameter variations
- Edge cases (empty input, null values, boundary conditions)
- Integration with TimeSeries objects
- Complex scenarios (regex patterns, multiple time series, etc.)

#### Serialization Tests (3-4 tests)
- `testWriteToAndReadFrom()` - Round-trip serialization (inherited from AbstractWireSerializingTestCase)
- `testToXContent()` - JSON representation
- `testToXContentRoundTrip()` - **Critical:** JSON → parse → deserialize round-trip (see [4-serialization-testing.md](4-serialization-testing.md))
- Verify `createTestInstance()` and `instanceReader()` implementations

#### Factory & Integration Tests (4-5 tests)
- `testFromArgs()` - Factory method with valid arguments
- `testPipelineStageFactory()` - PipelineStageFactory integration
- `testPipelineStageFactoryReadFrom_StreamInput()` - Stream deserialization
- Error cases for invalid arguments

#### Property Tests (2-3 tests)
- `testGetName()` - Verify stage name constant
- `testNullInputThrowsException()` - Null safety (use `TestUtils.assertNullInputThrowsException()`)
- `testEqualsAndHashCode()` - Equality contracts (if implemented)

### Template Structure

```java
public class YourStageTests extends AbstractWireSerializingTestCase<YourStage> {

    // ============ Core Functionality Tests ============

    public void testBasicOperation() {
        YourStage stage = new YourStage(/* params */);

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("tag", "value");
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts));

        // Assertions
        assertEquals(1, result.size());
    }

    public void testWithMultipleInputs() { /* ... */ }
    public void testEdgeCase() { /* ... */ }
    public void testWithEmptyInput() { /* ... */ }

    // ============ Null/Error Handling ============

    public void testNullInputThrowsException() {
        YourStage stage = new YourStage(/* params */);
        assertNullInputThrowsException(stage, "your_stage_name");
    }

    // ============ Factory Tests ============

    public void testFromArgs() {
        Map<String, Object> args = Map.of("param1", "value1");
        YourStage stage = YourStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("your_stage_name", stage.getName());
    }

    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(YourStage.NAME));
        Map<String, Object> args = Map.of("param1", "value1");
        PipelineStage stage = PipelineStageFactory.createWithArgs(YourStage.NAME, args);
        assertTrue(stage instanceof YourStage);
    }

    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        YourStage original = new YourStage(/* params */);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(YourStage.NAME);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertTrue(restored instanceof YourStage);
            }
        }
    }

    // ============ Serialization Tests ============

    public void testToXContent() throws IOException {
        YourStage stage = new YourStage(/* params */);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertTrue(json.contains("\"param1\":\"value1\""));
        }
    }

    // ============ Property Tests ============

    public void testGetName() {
        YourStage stage = new YourStage(/* params */);
        assertEquals("your_stage_name", stage.getName());
    }

    // ============ Required Overrides for AbstractWireSerializingTestCase ============

    @Override
    protected YourStage createTestInstance() {
        return new YourStage(
            randomAlphaOfLength(5),  // Random parameters for fuzz testing
            randomInt(100)
        );
    }

    @Override
    protected Writeable.Reader<YourStage> instanceReader() {
        return YourStage::readFrom;
    }
}
```

### Key Testing Utilities

```java
// Create test labels
ByteLabels labels = ByteLabels.fromStrings("key1", "value1", "key2", "value2");

// Create test samples
List<Sample> samples = List.of(
    new FloatSample(1000L, 10.0),
    new FloatSample(2000L, 20.0)
);

// Create TimeSeries
TimeSeries ts = new TimeSeries(samples, labels, minTime, maxTime, step, null);

// Test null input
assertNullInputThrowsException(stage, "stage_name");

// Factory arguments
Map<String, Object> args = Map.of("param", "value");
```

---

## 2. Plan Node Tests

**Location:** `src/test/java/org/opensearch/tsdb/lang/m3/m3ql/plan/nodes/YourPlanNodeTests.java`

**Pattern:** Extend `BasePlanNodeTests`

**Example:** `ExcludeByTagPlanNodeTests.java`

### Required Test Categories

#### Creation & Properties (5-7 tests)
- `testPlanNodeCreation()` - Basic construction with parameters
- `testGetters()` - Verify all getter methods
- `testGetExplainName()` - Verify explain format

#### Visitor Pattern (2 tests)
- `testVisitorAccept()` - Test accept() method with mock visitor
- `testVisitorReturnsCorrectType()` - Verify visitor returns expected type

#### Factory Method Tests (5-7 tests)
- `testFactoryMethod()` - Valid inputs
- `testFactoryMethodWithVariations()` - Different parameter combinations
- `testFactoryMethodThrowsOnInvalidInput()` - Error cases
- **Critical:** Factory methods must use `Utils.stripDoubleQuotes()` to handle quoted values from parser

#### Children Management (1-2 tests)
- `testChildrenManagement()` - Adding children to plan nodes

### Template Structure

```java
public class YourPlanNodeTests extends BasePlanNodeTests {

    // ============ Creation & Properties ============

    public void testPlanNodeCreation() {
        YourPlanNode node = new YourPlanNode(1, "param1", List.of("param2"));

        assertEquals(1, node.getId());
        assertEquals("param1", node.getParam1());
        assertEquals(List.of("param2"), node.getParam2());
        assertEquals("YOUR_NODE(param1=...,param2=...)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testGetExplainName() {
        YourPlanNode node = new YourPlanNode(1, "value", List.of("a", "b"));
        assertEquals("YOUR_NODE(param=value,list=a, b)", node.getExplainName());
    }

    // ============ Visitor Pattern ============

    public void testVisitorAccept() {
        YourPlanNode node = new YourPlanNode(1, "param", List.of("value"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit YourPlanNode", result);
    }

    // ============ Factory Method Tests ============

    public void testFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("yourFunction");
        functionNode.addChildNode(new ValueNode("\"param1\""));
        functionNode.addChildNode(new ValueNode("\"param2\""));

        YourPlanNode node = YourPlanNode.of(functionNode);

        assertEquals("param1", node.getParam1());  // Quotes stripped by Utils.stripDoubleQuotes()
        assertEquals(List.of("param2"), node.getParam2());
    }

    public void testFactoryMethodThrowsOnNoChildren() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("yourFunction");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> YourPlanNode.of(functionNode)
        );
        assertTrue(exception.getMessage().contains("must specify"));
    }

    public void testFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("yourFunction");
        functionNode.addChildNode(new FunctionNode()); // Wrong type

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> YourPlanNode.of(functionNode)
        );
        assertTrue(exception.getMessage().contains("must be a value"));
    }

    // ============ Children Management ============

    public void testChildrenManagement() {
        YourPlanNode node = new YourPlanNode(1, "param", List.of());
        M3PlanNode child = new YourPlanNode(2, "child", List.of());

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().getFirst());
    }

    // ============ Mock Visitor ============

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(YourPlanNode planNode) {
            return "visit YourPlanNode";
        }
    }
}
```

---

## 3. SourceBuilderVisitor Integration Tests

**Location:** `src/test/java/org/opensearch/tsdb/lang/m3/dsl/SourceBuilderVisitorTests.java`

**Pattern:** Add tests to existing test class

**Example:** Tests in `SourceBuilderVisitorTests.java` for ExcludeByTag

### Required Tests (3-4 tests)

```java
public void testYourPlanNodeWithOneChild() {
    YourPlanNode planNode = new YourPlanNode(1, "param");
    planNode.addChild(createMockFetchNode(2));

    // Should not throw an exception
    assertNotNull(visitor.visit(planNode));
}

public void testYourPlanNodeWithNoChildren() {
    YourPlanNode planNode = new YourPlanNode(1, "param");

    IllegalStateException exception = expectThrows(
        IllegalStateException.class,
        () -> visitor.visit(planNode)
    );
    assertEquals("YourPlanNode must have exactly one child", exception.getMessage());
}

public void testYourPlanNodeWithTwoChildren() {
    YourPlanNode planNode = new YourPlanNode(1, "param");
    planNode.addChild(createMockFetchNode(2));
    planNode.addChild(createMockFetchNode(3));

    IllegalStateException exception = expectThrows(
        IllegalStateException.class,
        () -> visitor.visit(planNode)
    );
    assertEquals("YourPlanNode must have exactly one child", exception.getMessage());
}
```

---

## 4. End-to-End M3QL Tests

**Critical:** Each M3QL test requires exactly **4 files** in **4 different folders**

### File Structure

```
src/test/resources/org/opensearch/tsdb/lang/m3/data/
├── queries/N.m3ql    # The M3QL query
├── ast/N.txt         # Expected AST (parser output)
├── plan/N.txt        # Expected Plan Tree (planner output)
└── dsl/N.dsl         # Expected OpenSearch DSL (translator output)
```

### Finding the Next Test Number

```bash
ls -1 src/test/resources/org/opensearch/tsdb/lang/m3/data/queries/ | sort -V | tail -1
# Output: 21.m3ql (so use 22 for new test)
```

### File 1: queries/N.m3ql

```m3ql
# Comment describing what this test validates
fetch name:"request_count" | perSecond | yourFunction param1 "param2" "param3"
```

### File 2: ast/N.txt

Abstract Syntax Tree representation:

```
ROOT
  PIPELINE
    FUNCTION(fetch)
      TAG_KEY(name)
        TAG_VALUE(request_count)
    FUNCTION(perSecond)
    FUNCTION(yourFunction)
      VALUE(param1)        # First arg unquoted (tag name)
      VALUE("param2")      # String args keep quotes
      VALUE("param3")
```

**Critical Rules:**
- Tag names (first argument to most functions) are NOT quoted
- String values ARE quoted (as parser outputs them)
- Function names are lowercase
- Indentation: 2 spaces per level

### File 3: plan/N.txt

Plan tree representation:

```
YOUR_FUNCTION(param1, param2, param3)
  PER_SECOND
    FETCH({name=[request_count]}, !{})
```

**Format:**
- Node names in UPPER_SNAKE_CASE
- Parameters in parentheses
- Child nodes indented 2 spaces
- Tree structure shows pipeline stages

### File 4: dsl/N.dsl

Full OpenSearch DSL JSON:

```json
{
  "size" : 0,
  "query" : {
    "time_range_pruner" : {
      "min_timestamp" : 1000000000,
      "max_timestamp" : 1001000000,
      "query" : {
        "bool" : {
          "filter" : [
            {
              "range" : {
                "timestamp_range" : {
                  "from" : 1000000000,
                  "to" : 1001000000,
                  "include_lower" : true,
                  "include_upper" : false,
                  "boost" : 1.0
                }
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:request_count"
                ],
                "boost" : 1.0
              }
            }
          ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      },
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 1000000000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "per_second"
          },
          {
            "type" : "your_stage_name",
            "param1" : "param1",
            "param2" : [
              "param2",
              "param3"
            ]
          }
        ]
      }
    }
  }
}
```

**Notes:**
- Timestamps are fixed test constants (START_TIME, END_TIME, STEP)
- If stage is the only stage, may not have `0_coordinator` aggregation
- Stage type uses snake_case
- JSON formatting must match (2-space indent)

---

## Test Coverage Checklist

For each new M3QL pipeline stage:

- [ ] **Stage Tests** (20+ tests)
  - [ ] Core functionality (10-15 tests)
  - [ ] Edge cases (empty, null, boundaries)
  - [ ] Serialization (writeToAndReadFrom, toXContent)
  - [ ] Factory integration (fromArgs, PipelineStageFactory)
  - [ ] Null input throws exception
  - [ ] equals() and hashCode() if implemented

- [ ] **Plan Node Tests** (19+ tests)
  - [ ] Creation and properties (5-7 tests)
  - [ ] Visitor pattern with mock (2 tests)
  - [ ] Factory method (valid and error cases) (5-7 tests)
  - [ ] Children management (1-2 tests)

- [ ] **SourceBuilderVisitor Integration** (4 tests)
  - [ ] Valid child count test
  - [ ] Invalid child count tests (0, 2+)

- [ ] **End-to-End M3QL Test** (4 files)
  - [ ] queries/N.m3ql
  - [ ] ast/N.txt
  - [ ] plan/N.txt
  - [ ] dsl/N.dsl

- [ ] **All Tests Pass**
  - [ ] `./gradlew check` succeeds
  - [ ] No existing tests broken

**Total:** ~48 tests for a complete pipeline stage implementation

---

## Common Pitfalls

### ❌ Don't
- Forget to create all 4 files for M3QL tests
- Quote tag names in AST files (first argument to functions)
- Forget to use `Utils.stripDoubleQuotes()` in factory methods
- Run tests without fixing code formatting first
- Create tests without following existing patterns
- **Skip round-trip serialization tests** (see [4-serialization-testing.md](4-serialization-testing.md))
- Use `enum.name()` in `toXContent()` without lowercasing

### ✅ Do
- Follow existing test patterns (PerSecondStageTests, AliasByTagsPlanNodeTests)
- Test both happy path and error cases
- **Always include round-trip tests:** serialize → parse → deserialize → verify
- Verify serialization/deserialization works for all variants
- Run `./gradlew spotlessApply` before tests
- Run `./gradlew check` before committing
- Add comprehensive coverage (20+ tests per class)
- Reference [4-serialization-testing.md](4-serialization-testing.md) for serialization patterns

---

## Example: Complete Implementation

**Reference:** ExcludeByTag implementation

Files created:
- `ExcludeByTagStage.java` - Stage implementation
- `ExcludeByTagStageTests.java` - 20 tests
- `ExcludeByTagPlanNode.java` - Plan node
- `ExcludeByTagPlanNodeTests.java` - 19 tests
- `SourceBuilderVisitorTests.java` - Added 4 tests
- `queries/22.m3ql` - End-to-end test query
- `ast/22.txt` - Expected AST
- `plan/22.txt` - Expected plan
- `dsl/22.dsl` - Expected DSL

**Total:** 48 tests, full pipeline coverage ✅
