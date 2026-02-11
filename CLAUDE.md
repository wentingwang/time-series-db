# OpenSearch TSDB Plugin

Time-series database plugin for OpenSearch with M3QL query language support.

## Build & Test
```bash
./gradlew precommit      # Verify formatting changes
./gradlew check          # Run all tests
./gradlew run            # Start cluster with plugin
./gradlew jmh            # Run JMH benchmarks
```

## Key Components

### Core Engine
- **TSDBEngine**: Custom engine for time-series storage (XOR compression)
- **Head**: In-memory active time series (src/main/java/org/opensearch/tsdb/core/head/)
- **ClosedChunkIndex**: Disk-based compressed time series (src/main/java/org/opensearch/tsdb/core/index/closed/)

### Query Language
- **M3QL**: Query language with pipeline stages (fetch, sum, moving, alias, etc.)
- **Parser**: JavaCC-based parser (src/main/java/org/opensearch/tsdb/lang/m3/m3ql/parser/)
- **Translator**: M3QL → OpenSearch DSL (src/main/java/org/opensearch/tsdb/lang/m3/dsl/)

### Aggregators
- **TimeSeriesUnfoldAggregator**: Shard-level aggregation with pipeline stages
- **TimeSeriesCoordinatorAggregator**: Coordinator-level aggregation

### Benchmarks
- Located in src/jmhTest/java/org/opensearch/search/aggregator/
- **BaseTSDBBenchmark**: Base infrastructure class
- **TimeSeriesUnfoldAggregationBenchmark**: Benchmark for unfold aggregation

## Project Structure
```
src/main/java/org/opensearch/tsdb/
   core/          # Storage engine (chunks, indexes, compression)
   lang/m3/       # M3QL language (parser, planner, stages)
   query/         # Aggregators and REST endpoints
```

## Common Tasks
- Run cluster: `./gradlew run -PnumNodes=2`
- JMH benchmarks: `./gradlew jmh`
- JavaCC regenerate: `./gradlew generateJavaCC`
- Format code: `./gradlew spotlessApply`

## Testing
- Unit: src/test/
- Integration: src/javaRestTest/, src/internalClusterTest/
- Test framework: YAML-based (see src/*/resources/test_cases/)
- **Coverage Requirement**: ≥85% instruction coverage (enforced by JaCoCo)

---

## Coding Task Protocol

### Phase 1: Context Gathering (USER provides this)

Before starting any coding task, the user will provide:

#### 1.1 Reading Materials
```
Required reading (Claude MUST read these files first):
- [ ] Main class to modify: src/main/java/org/opensearch/tsdb/.../ClassName.java
- [ ] Existing tests: src/test/java/org/opensearch/tsdb/.../ClassNameTests.java
- [ ] Similar implementations: [list similar classes]
- [ ] Related documentation: [if applicable]
```

#### 1.2 Coverage/Reports
```
Pre-task reports (Claude should review):
- [ ] Coverage report: build/reports/jacoco/test/html/index.html
- [ ] Test results: build/reports/tests/test/index.html
- [ ] Build status: Current build passing? Yes/No
```

#### 1.3 Investigation Checklist
```
Before writing code, Claude must investigate:
- [ ] Read all files in "Reading Materials" section
- [ ] Search for similar patterns: grep -r "PatternName" src/
- [ ] Check for TODOs/FIXMEs: grep -r "TODO\|FIXME" src/main/java/org/opensearch/tsdb/
- [ ] Review git history: git log --oneline --grep="RelatedFeature" -n 20
- [ ] Understand test patterns: Read 2-3 existing test files in same package
- [ ] Check dependencies: grep -r "import.*ClassName" src/
```

#### 1.4 Test Requirements (for TDD tasks)
```
Tests to implement (in order):
1. [ ] testConstructor - Verify object creation with valid inputs
2. [ ] testNullHandling - Verify null parameter handling
3. [ ] testSerialization - Verify serialization/deserialization
4. [ ] testEquals - Verify equals() and hashCode()
5. [ ] testEdgeCases - Verify boundary conditions

Success criteria:
- All tests pass
- Coverage: ≥85% instruction coverage (project requirement)
- No regressions in existing tests
```

---

### Phase 2: Investigation (CLAUDE executes automatically)

**STOP and complete ALL of these before writing ANY code:**

#### 2.1 Baseline Verification
```bash
# Verify current state is clean
./gradlew test
./gradlew jacocoTestReport

# If tests fail: STOP and report to user
# If coverage < 85%: Note baseline coverage
```

#### 2.2 Pattern Study
For each file in "Reading Materials":
- Read the entire file
- Note coding patterns, naming conventions, structure
- Identify similar classes/methods (especially other Records, aggregators, etc.)
- Look for comments explaining design decisions
- Check for JavaCC grammar files if parser-related

#### 2.3 Dependency Analysis
```bash
# Check what this code depends on
grep -r "import.*TargetClass" src/

# Check what depends on this code
grep -r "new TargetClass\|TargetClass\." src/

# Check for special dependencies:
# - HyperLogLogPlusPlus (see Known Issues)
# - BigArrays
# - Custom serialization (StreamInput/StreamOutput)
# - OpenSearch framework classes (InternalAggregation, etc.)
```

#### 2.4 Investigation Report
Before proceeding, Claude reports:
```
Investigation Summary:
1. Baseline: Tests passing? [Yes/No] | Coverage: [X%]
2. Patterns found: [List 2-3 key patterns from similar classes]
3. Dependencies: [Note any complex dependencies like HyperLogLog]
4. Risks/Concerns: [Pre-existing bugs, unclear requirements, etc.]
5. Recommended approach: [Brief plan]

Ready to proceed? [Wait for user confirmation if risks found]
```

---

### Phase 3: Implementation (CLAUDE executes incrementally)

**Golden Rule: ONE test at a time, run after EACH change**

#### 3.1 Test-Driven Development Cycle

For each test in "Test Requirements":

```bash
# Step 1: Write ONE test method
# Step 2: Run ONLY that test
./gradlew test --tests org.opensearch.tsdb.ClassName.testMethodName

# Step 3: Verify result
# ✅ PASS → Document what was tested, move to next test
# ❌ FAIL → Debug (see Phase 4), do NOT write more tests until fixed

# Step 4: Commit mental checkpoint
# Document: What works, what's covered, what's next
```

#### 3.2 Incremental Testing Rules

**MUST follow these rules:**
- ✅ Write one test → Run it → Verify → Proceed
- ❌ NEVER write multiple tests before running any
- ❌ NEVER retry the same test without investigation
- ❌ NEVER skip a failing test to "come back later"

#### 3.3 Code Review Checkpoints

After every 3 tests (or major milestone):
```bash
# Run full test suite for the class
./gradlew test --tests org.opensearch.tsdb.ClassName

# Check coverage
./gradlew jacocoTestReport
# Then review: build/reports/jacoco/test/html/org.opensearch.tsdb.package/ClassName.java.html

# Self-review:
# - Are tests following existing patterns in this package?
# - Is coverage improving?
# - Any code smells or duplication?
```

---

### Phase 4: Failure Response Protocol

**When ANY test fails:**

#### 4.1 Immediate Actions
1. **STOP** - Do not write more code
2. **READ** - Read the full error message and stack trace
3. **LOCATE** - Find exact line number where failure occurs
4. **UNDERSTAND** - What is the root cause?

#### 4.2 Failure Decision Tree

```
Test Failed
    ↓
Is it the SAME error as before?
    ├─ YES → Stop and investigate root cause
    │         Do NOT retry same approach
    │         ↓
    │         Is it a pre-existing bug in production code?
    │         ├─ YES → Document limitation, skip this test, ask user
    │         └─ NO  → Debug test logic, fix, retry
    │
    └─ NO  → Is it a new error?
              ├─ Typo/syntax → Fix and retry
              ├─ Logic error → Debug and fix
              └─ Unclear → Ask user for clarification
```

#### 4.3 Red Flags (Stop and Ask User)

🚩 Stop immediately and ask user if:
- Same `ClassCastException` / `NPE` / error appears 2+ times
- Test requires modifying production code with pre-existing bugs
- Unclear if behavior is bug or feature
- Multiple valid implementation approaches exist
- Tests don't match existing patterns in the codebase
- HyperLogLog serialization issues appear (see Known Issues)

---

### Phase 5: Completion Checklist

Before declaring task complete:

```bash
# 1. All required tests implemented and passing
./gradlew test --tests org.opensearch.tsdb.TargetClassTests
# Result: X/X tests passing

# 2. Full test suite still passes (no regressions)
./gradlew test
# Result: [Total] tests passing, 0 failures

# 3. Coverage meets requirements
./gradlew jacocoTestReport
# Result: X% coverage (target: ≥85%)

# 4. Code quality
# - [ ] Follows existing patterns (check similar classes)
# - [ ] No duplication
# - [ ] Clear test names (test*, descriptive)
# - [ ] Documented any limitations (comments)
# - [ ] Proper imports (OpenSearch test framework)

# 5. Documentation
# - [ ] Updated comments if needed
# - [ ] Noted any skipped tests with reasons
# - [ ] Recorded any discovered bugs in "Known Issues" section
```

---

## Project-Specific Testing Patterns

### Record Classes (Java Records)
```java
// Pattern: Test constructor and accessors
public void testRecordConstructor() {
    // Arrange
    var input1 = "value1";
    var input2 = 100L;

    // Act
    var record = new MyRecord(input1, input2);

    // Assert
    assertEquals(input1, record.field1());
    assertEquals(input2, record.field2());
}

// Pattern: Test serialization
public void testRecordSerialization() throws IOException {
    // Arrange
    var original = new MyRecord("value", 100L);

    // Act
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    var deserialized = new MyRecord(in);

    // Assert
    assertEquals(original, deserialized);
}
```

### Aggregator Classes
```java
// Pattern: Setup test infrastructure
@Override
protected AggregationBuilder createAggBuilderForTypeTest() {
    return new MyAggregationBuilder("test-agg");
}

// Pattern: Test aggregation
public void testAggregation() throws IOException {
    // Use OpenSearchTestCase helpers
    // Check InternalAggregationTestCase for patterns
}
```

### OpenSearch Test Framework
```java
// Always extend OpenSearchTestCase or its subclasses
public class MyTests extends OpenSearchTestCase {

    // Use framework methods:
    // - randomAlphaOfLength(10)
    // - randomInt(), randomLong()
    // - expectThrows(Exception.class, () -> {...})
    // - assertNotNull(), assertEquals(), assertTrue()
}
```

---

## Known Issues / Limitations

### Pre-existing Bugs

#### HyperLogLog Serialization (InternalTSDBStats.java:91)
**Issue**: `HyperLogLogPlusPlus.readFrom()` can return `HyperLogLogPlusPlusSparse`, which cannot be cast to `HyperLogLogPlusPlus`.

**Stack Trace**:
```
java.lang.ClassCastException: class HyperLogLogPlusPlusSparse cannot be cast to class HyperLogLogPlusPlus
    at InternalTSDBStats$ShardLevelStats.readSeriesSketch(InternalTSDBStats.java:91)
```

**Impact**: Cannot test serialization for classes containing `HyperLogLogPlusPlus` fields.

**Workaround**:
- Skip serialization tests for affected classes
- Test only constructor and accessors
- Document limitation in test comments:
```java
// Note: Serialization tests skipped due to HyperLogLog cast bug (InternalTSDBStats.java:91)
// This is a pre-existing issue where HyperLogLogPlusPlus.readFrom() returns HyperLogLogPlusPlusSparse
```

**Affected Classes**:
- `InternalTSDBStats.ShardLevelStats`
- Any class using `HyperLogLogPlusPlus` for serialization

**Status**: Known issue, outside scope of Record conversion work

---

## Session Template

Copy this for each new coding session:

```markdown
## Session: [Date] - [Task Name]

### Phase 1: Context (User Provides)
**Reading Materials:**
- [ ] src/main/java/org/opensearch/tsdb/.../ClassName.java
- [ ] src/test/java/org/opensearch/tsdb/.../ClassNameTests.java
- [ ] Similar: [list similar classes]

**Coverage/Reports:**
- [ ] Current coverage: build/reports/jacoco/test/html/index.html
- [ ] Baseline tests passing: Yes/No

**Investigation Checklist:**
- [ ] Run baseline tests: ./gradlew test
- [ ] Read all files in Reading Materials
- [ ] Check for TODOs: grep -r "TODO" src/main/java/org/opensearch/tsdb/path/
- [ ] Review test patterns: Read 2-3 test files in same package
- [ ] Check for HyperLogLog usage (see Known Issues)

**Test Requirements:**
1. [ ] Test 1: Description
2. [ ] Test 2: Description
3. [ ] Test 3: Description

**Success Criteria:**
- All tests pass
- Coverage ≥85%
- No regressions

---

### Phase 2: Investigation (Claude Reports)
**Baseline:**
- Tests passing: [Yes/No]
- Coverage: [X%]
- Build time: [Xs]

**Patterns Found:**
- Pattern 1: [e.g., "Records use constructor + accessors"]
- Pattern 2: [e.g., "Serialization uses StreamInput/StreamOutput"]
- Pattern 3: [e.g., "Tests extend OpenSearchTestCase"]

**Dependencies:**
- [e.g., "Uses HyperLogLogPlusPlus - see Known Issues"]
- [e.g., "Implements Writeable interface"]

**Risks:**
- [e.g., "HyperLogLog serialization may fail"]
- [e.g., "None identified"]

**Recommended Approach:**
[Brief 2-3 sentence plan]

**Ready to proceed?** [Wait for user if risks found]

---

### Phase 3: Implementation Log
- [ ] Test 1: [Status] [Time] [Notes]
- [ ] Test 2: [Status] [Time] [Notes]
- [ ] Test 3: [Status] [Time] [Notes]

Example:
- [x] testConstructor: ✅ PASS (0.5s) - Basic construction working
- [x] testSerialization: ❌ FAIL → HyperLogLog cast error → Skipped (documented)
- [x] testNullHandling: ✅ PASS (0.3s)

---

### Phase 4: Failures (if any)
**Failure 1:**
- Test: [test name]
- Error: [error message]
- Root cause: [analysis]
- Resolution: [how fixed or why skipped]

---

### Phase 5: Completion
- [ ] All required tests passing: [X/Y]
- [ ] No regressions: ./gradlew test → [Total] tests passing
- [ ] Coverage: [X%] (target: ≥85%)
- [ ] Code quality reviewed
- [ ] Limitations documented

**Final Coverage Report:** build/reports/jacoco/test/html/index.html
```

---

## Meta: Improving This Protocol

After each session, consider:
- What went wrong that this protocol didn't catch?
- What patterns emerged that should be documented?
- What checklist items should be added?
- Should new issues be added to "Known Issues"?

Update this file to make Claude more autonomous over time.

**Recent Updates:**
- 2026-02-10: Added coding protocol, known HyperLogLog issue, testing patterns
