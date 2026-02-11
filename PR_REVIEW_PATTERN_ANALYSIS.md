# PR Review Pattern Analysis Report

## Summary Statistics
- Total PRs analyzed: 20 merged PRs
- Total review comments extracted: ~50+ detailed review comments
- philiplhchan comments: ~35 (weighted 3x)
- Other reviewer comments: ~15
- Time period: 2026-01-29 to 2026-02-10
- Primary repository: opensearch-project/time-series-db

## Extracted Rules by Category

### 1. Architecture & Design Patterns [8 rules]

#### Rule 1.1: Avoid Modifying Immutable Collections
**Priority:** CRITICAL
**Frequency:** 1 occurrence (PR #48)
**Primary Reviewer:** philiplhchan (implied by fix)

**Rule:** Never modify immutable collections directly. Create new collections instead.
**Rationale:** Modifying immutable collections throws runtime exceptions and is hard to track. Creating new collections is safer and more predictable.
**Example from PR #48:** UnionStage was attempting to modify an immutable list, causing errors. Fixed by creating a new ArrayList and adding both collections.
**How to Check:**
- Look for `Collections.unmodifiableList()` return values being modified
- Check for `List.of()` or similar immutable collection patterns
- Ensure any collection modification creates a new instance

#### Rule 1.2: Coordinator-Only vs Pushdown Stages
**Priority:** HIGH
**Frequency:** 3 occurrences (PRs #5, #6)
**Primary Reviewer:** philiplhchan

**Rule:** Carefully design whether a pipeline stage is coordinator-only or supports pushdown. Default should be coordinator-only (safer), with explicit opt-in for pushdown support.
**Rationale:** Pushdown optimization requires careful consideration of reduce() semantics. Stages that support pushdown must handle shard-level and coordinator-level execution correctly.
**Example from PR #5:** philiplhchan suggested: "we should consider making pushdown default to `false`, and have each stage that can support pushdown override to true. it's probably safer that way."
**How to Check:**
- Review `isCoordinatorOnly()` implementation
- Verify `reduce()` method handles multi-shard aggregation correctly
- Check if stage semantics work correctly when executed on shards vs coordinator
- Ensure documentation explains when reduce() vs process() is called

#### Rule 1.3: Stateful Operations Require Coordinator-Only Execution
**Priority:** CRITICAL
**Frequency:** 2 occurrences (PR #8 - ChangedStage)
**Primary Reviewer:** Code design decision

**Rule:** Operations requiring prior state (like "changed" which needs previous value) must not support concurrent segment search and should document this limitation.
**Rationale:** Distributed execution breaks stateful operations that depend on ordered processing.
**Example from PR #8:** ChangedStage explicitly documents "Does not support concurrent segment search (requires prior sample information)"
**How to Check:**
- Identify stages that track state across samples/series
- Ensure `isCoordinatorOnly()` returns true for stateful stages
- Document the requirement in class javadoc

#### Rule 1.4: Data Source Specific Requirements
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #20)
**Primary Reviewer:** philiplhchan

**Rule:** When adding data source specific requirements (like M3QL step size), add TODOs to tie requirements to engine variant and document future extensibility.
**Rationale:** Enables future support for different data sources (M3, Prometheus, etc.) without breaking existing functionality.
**Example from PR #20:** philiplhchan: "step size is only required for m3ql, prometheus data source would not have this or require it... add a note/todo to specify that if we add a different data source type, then we should tie this requirement to the engine variant"
**How to Check:**
- Look for data source specific validation logic
- Check if requirements are hardcoded vs configurable
- Add TODOs for future extensibility
- Update method names to indicate specificity (e.g., `validateM3QLRequiredSettings`)

#### Rule 1.5: Optimization Opportunities Should Be Documented
**Priority:** MEDIUM
**Frequency:** 2 occurrences (PRs #19, #5)
**Primary Reviewer:** philiplhchan

**Rule:** When identifying optimization opportunities during review, document them as TODOs in code for future implementation.
**Rationale:** Captures tribal knowledge and prevents duplicate discovery work.
**Example from PR #19:** philiplhchan noted "CountStage should also massively benefit from constantLine" when reviewing FloatSampleList.ConstantList
**Example from PR #5:** TODO added: "Optimize performance - Replace O(N log N) full sort with O(N log K) heap-based partial selection"
**How to Check:**
- Review for performance optimization opportunities
- Document algorithmic improvements in TODO comments
- Reference specific classes that could benefit

#### Rule 1.6: Mutable Class Usage Should Be Justified
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** philiplhchan

**Rule:** If a class is mutable, use mutation for efficiency (return `this`). Don't create unnecessary new instances.
**Rationale:** Mutable classes exist for performance reasons; allocating new objects defeats the purpose.
**Example from PR #19:** philiplhchan: "if it's mutable, why wouldn't we just update the value and `return this`?" (regarding MutableFloatSample.merge())
**How to Check:**
- Check if mutable classes create new instances in methods
- Verify that mutation is actually used for performance
- Consider if immutability would be better for the use case

#### Rule 1.7: Reduce Semantics Must Be Clear
**Priority:** HIGH
**Frequency:** 2 occurrences (PR #6)
**Primary Reviewer:** philiplhchan

**Rule:** Document reduce() behavior clearly, especially regarding ordering guarantees across shards. Clarify when aggregations are pre-ordered vs unordered.
**Rationale:** Prevents incorrect assumptions about cross-shard data ordering.
**Example from PR #6:** philiplhchan asked "does caller of `reduce` already correctly order the aggregations before calling this reduce? or... does `aggregations.size() == 1` all the time?" Discussion revealed ordering is not guaranteed.
**How to Check:**
- Document reduce() ordering assumptions
- Clarify if results are deterministic across shards
- Explain difference between reduce() and process() execution paths

#### Rule 1.8: Avoid Unnecessary Array Copies
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #26)
**Primary Reviewer:** zhaih

**Rule:** Avoid using `toList()` in non-test production code. Work directly with SampleList interfaces instead.
**Rationale:** Prevents unnecessary array allocations and copies in hot paths.
**Example from PR #26:** zhaih: "Probably better follow this? [links to AvgStage pattern] Ideally we don't want to use `toList()` in non-test code"
**How to Check:**
- Search for `.toList()` calls in production code
- Use SampleList interface methods instead
- Follow established patterns in similar stages

---

### 2. Code Quality Standards [6 rules]

#### Rule 2.1: Method Names Must Reflect Actual Behavior
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** philiplhchan

**Rule:** When changing implementation semantics (e.g., binary search → generic search), update both method name and documentation consistently.
**Rationale:** Prevents confusion between interface contract and implementation details.
**Example from PR #19:** philiplhchan: "are we renaming to 'search' to decouple the impl from binary search? if yes, do we want to update the comment on line 64 too?" and "maybe let's be more clear (copy over the description... so we don't need to separately find the docs)"
**How to Check:**
- Review method renames for documentation updates
- Ensure javadoc matches actual implementation
- Copy full contract description, don't just reference other interfaces

#### Rule 2.2: Document Edge Cases and Limitations
**Priority:** MEDIUM
**Frequency:** 3 occurrences (PRs #6, #8, #19)
**Primary Reviewer:** Multiple

**Rule:** Explicitly document behavioral edge cases, especially around ordering, null handling, and distributed execution.
**Rationale:** Prevents misuse and clarifies expected behavior in non-obvious scenarios.
**Example from PR #6:** Documentation added: "Note: The reduce() operation works the same for both head and tail modes. If reduce() is called directly without prior sorting, we cannot guarantee the order of time series across shards, so this will return random N series..."
**How to Check:**
- Document null/empty input behavior
- Clarify ordering guarantees
- Explain interaction with other stages

#### Rule 2.3: Follow Existing Code Patterns
**Priority:** HIGH
**Frequency:** 2 occurrences (PRs #26, #8)
**Primary Reviewer:** zhaih, team consensus

**Rule:** When implementing new features, follow established patterns from similar existing code. Reference specific examples in code.
**Rationale:** Maintains codebase consistency and leverages battle-tested approaches.
**Example from PR #26:** zhaih links to AvgStage pattern for proper SampleList usage
**How to Check:**
- Search for similar implementations before writing new code
- Reference existing patterns in PR descriptions
- Ask "how do we do this elsewhere?" before inventing new approaches

#### Rule 2.4: Comments Should Explain "Why" Not "What"
**Priority:** MEDIUM
**Frequency:** Implicit across multiple PRs
**Primary Reviewer:** General best practice observed

**Rule:** Comments should explain reasoning, trade-offs, and non-obvious decisions rather than restating code.
**Rationale:** Code shows what; comments should explain why.
**Example from PR #19:** TODO comment explains: "If we know original copy will be discarded then we can let FloatSampleList take an additional 'start' field to avoid array copy at all"
**How to Check:**
- Review comments for value-add
- Explain architectural decisions
- Document performance trade-offs

#### Rule 2.5: Update Comments When Changing Functionality
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** Code hygiene

**Rule:** When test behavior changes (like removing mixed sample type support), update comments to reflect new reality.
**Rationale:** Outdated comments mislead future maintainers.
**Example from PR #19:** Comment added explaining why mixed sample type testing was removed: "Now we don't really explicitly support mix type of samples from our interface for simplicity... We should revisit if we see such a need"
**How to Check:**
- Review changed tests for comment updates
- Explain why previous approach was abandoned
- Document future considerations

#### Rule 2.6: Avoid Magic Numbers
**Priority:** LOW
**Frequency:** Observed in constants usage
**Primary Reviewer:** General best practice

**Rule:** Define constants with clear names rather than using magic numbers.
**Rationale:** Improves readability and maintainability.
**Example from codebase:** `BUILDER_INITIAL_CAPACITY = 16` instead of hardcoded 16
**How to Check:**
- Replace numeric literals with named constants
- Add javadoc explaining constant values
- Group related constants together

---

### 3. Correctness & Bug Prevention [7 rules]

#### Rule 3.1: Validate Input Invariants
**Priority:** HIGH
**Frequency:** 3 occurrences (PRs #26, #19)
**Primary Reviewer:** philiplhchan, team discussion

**Rule:** Decide whether to validate input invariants (like min <= max, timestamps non-decreasing) based on whether they can be guaranteed through proper usage.
**Rationale:** Defensive validation prevents bugs but adds overhead. Use when invariants can't be guaranteed by API design.
**Example from PR #26:** philiplhchan: "do we need to validate min<=max?" Response: "If it's only used in range function, we do not need this validation [because] invariant still holds for all inputs"
**Example from PR #19:** philiplhchan: "should we validate timestamp cannot go backwards here? or that's not the responsibility of the list container class?"
**How to Check:**
- Identify critical invariants
- Determine if type system/API design can guarantee them
- Add validation when invariants can be violated
- Document assumptions in javadoc

#### Rule 3.2: Handle Null Inputs Explicitly
**Priority:** HIGH
**Frequency:** Observed in multiple stages
**Primary Reviewer:** Code pattern

**Rule:** Explicitly check for and handle null inputs with clear error messages. Use `NullPointerException` or `IllegalArgumentException` appropriately.
**Rationale:** Fail fast with clear messages rather than obscure NPEs later.
**Example from codebase:** `if (input == null) { throw new NullPointerException(getName() + " stage received null input"); }`
**How to Check:**
- Check all public method parameters
- Throw appropriate exception types
- Include stage/class name in error message

#### Rule 3.3: Document Mixed Type Limitations
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** philiplhchan, ZiwenWan

**Rule:** When removing support for mixed types (e.g., mixing FloatSample and SumCountSample), document the limitation and rationale.
**Rationale:** Clarifies API contracts and prevents incorrect usage.
**Example from PR #19:** philiplhchan asked "@ZiwenWan is this a problem?" regarding removal of mixed sample type testing. Comment added explaining "We should revisit if we see such a need of mixing sample types"
**How to Check:**
- Document type homogeneity requirements
- Explain why mixed types aren't supported
- Add TODO if future support might be needed

#### Rule 3.4: Validate Constructor Arguments
**Priority:** HIGH
**Frequency:** Standard pattern across multiple PRs
**Primary Reviewer:** Code convention

**Rule:** Validate constructor arguments immediately, throwing `IllegalArgumentException` with descriptive messages.
**Rationale:** Establishes object invariants at construction time.
**Example from multiple stages:**
```java
if (limit <= 0) {
    throw new IllegalArgumentException("Limit must be positive, got: " + limit);
}
if (mode == null) {
    throw new IllegalArgumentException("Mode cannot be null");
}
```
**How to Check:**
- Validate all constructor parameters
- Include actual values in error messages
- Check for null, range violations, invalid states

#### Rule 3.5: Check Empty/Size Edge Cases
**Priority:** MEDIUM
**Frequency:** Standard pattern
**Primary Reviewer:** Code convention

**Rule:** Always handle empty collections and size edge cases explicitly.
**Rationale:** Prevents index out of bounds and clarifies behavior.
**Example from multiple stages:**
```java
if (input.isEmpty()) {
    return new ArrayList<>();
}
int size = Math.min(limit, input.size());
```
**How to Check:**
- Handle empty input explicitly
- Use Math.min/max for boundary calculations
- Test with size < limit and size > limit

#### Rule 3.6: Use Assertions for Internal Invariants
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** Code pattern

**Rule:** Use assertions for internal invariants that should never be violated (like array bounds).
**Rationale:** Catches programming errors in development without runtime cost in production.
**Example from FloatSampleList:**
```java
public double getValue(int index) {
    assert index >= 0 && index < size;
    return values[index];
}
```
**How to Check:**
- Use assertions for "should never happen" conditions
- Don't use for user input validation
- Document assumptions

#### Rule 3.7: Safe Setting Changes
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #15)
**Primary Reviewer:** philiplhchan, karenyrx

**Rule:** When removing settings, follow safe deployment procedure: 1) Set to null in all clusters, 2) Deploy binary, 3) Verify no errors.
**Rationale:** Prevents deployment failures and ensures smooth rollout.
**Example from PR #15:** Discussion about removing TSDB_ENGINE_AGGREGATION_CIRCUIT_BREAKER_WARN_THRESHOLD setting included deployment procedure validation.
**How to Check:**
- Document setting removal procedure
- Test in staging before production
- Verify "archived" settings don't cause errors

---

### 4. Testing Standards [5 rules]

#### Rule 4.1: Add Tests to Existing Test Files
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #35)
**Primary Reviewer:** philiplhchan

**Rule:** When adding new functionality to existing test framework, add test coverage by extending existing test files rather than just documenting capability.
**Rationale:** Ensures new features are actually tested and prevents regression.
**Example from PR #35:** philiplhchan: "can you add this to 1 existing test to verify it works" (referring to alias checking functionality)
**How to Check:**
- Identify existing test files that should cover new code
- Add at least one test case demonstrating usage
- Don't just document - validate with tests

#### Rule 4.2: Test Edge Cases
**Priority:** HIGH
**Frequency:** Observed across multiple PRs
**Primary Reviewer:** Code pattern

**Rule:** Test edge cases including: null values, empty collections, single element, boundary values, NaN handling.
**Rationale:** Edge cases are where bugs hide.
**Example from test patterns observed:**
- testBasicFunctionality
- testNullHandling
- testEdgeCases (single sample, empty series, all NaN values)
**How to Check:**
- Review test coverage for edge cases
- Include null/empty/single/boundary tests
- Test invalid inputs and error conditions

#### Rule 4.3: Test Serialization/Deserialization
**Priority:** HIGH
**Frequency:** Observed in multiple PRs
**Primary Reviewer:** Code convention

**Rule:** Always test serialization round-trip for classes implementing StreamInput/StreamOutput.
**Rationale:** Ensures distributed aggregation works correctly.
**Example pattern:**
```java
BytesStreamOutput out = new BytesStreamOutput();
original.writeTo(out);
StreamInput in = out.bytes().streamInput();
var deserialized = new MyClass(in);
assertEquals(original, deserialized);
```
**How to Check:**
- Test writeTo/readFrom round trip
- Verify all fields are preserved
- Test with various data combinations

#### Rule 4.4: Follow Existing Test Patterns
**Priority:** MEDIUM
**Frequency:** General observation
**Primary Reviewer:** Code convention

**Rule:** Follow established testing patterns for similar classes (e.g., InternalAggregationTestCase for aggregators).
**Rationale:** Leverages test infrastructure and maintains consistency.
**Example from codebase:** Aggregator tests extend InternalAggregationTestCase and implement `createAggBuilderForTypeTest()`
**How to Check:**
- Find similar test classes
- Extend appropriate test base classes
- Use framework helper methods

#### Rule 4.5: Coverage Requirements
**Priority:** CRITICAL
**Frequency:** Enforced by CI (from CLAUDE.md)
**Primary Reviewer:** Automated (JaCoCo)

**Rule:** Maintain ≥85% instruction coverage as enforced by JaCoCo.
**Rationale:** Project policy to ensure thorough testing.
**Example:** Project enforces this via gradle check and reports to build/reports/jacoco/test/html/
**How to Check:**
- Run `./gradlew jacocoTestReport`
- Review coverage report
- Add tests for uncovered code paths

---

### 5. Performance [5 rules]

#### Rule 5.1: Avoid Unnecessary Object Allocation in Hot Paths
**Priority:** HIGH
**Frequency:** 2 occurrences (PRs #19, #26)
**Primary Reviewer:** philiplhchan, zhaih

**Rule:** Minimize object allocation in performance-critical code. Use iterator views, mutable reusable objects, and avoid toList() conversions.
**Rationale:** Reduces GC pressure and improves throughput.
**Example from PR #19:** Iterator returns mutable view instead of creating new Sample objects. Comment: "This implementation return a view, so do not store it without copy"
**Example from PR #26:** "Ideally we don't want to use `toList()` in non-test code"
**How to Check:**
- Profile hot paths
- Look for repeated object creation
- Use object pooling or mutable views
- Benchmark before/after changes

#### Rule 5.2: Use Efficient Data Structures
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** Design decision

**Rule:** Choose data structures optimized for access patterns (e.g., parallel arrays for timestamps/values vs List<Sample>).
**Rationale:** FloatSampleList uses parallel arrays for better memory locality and performance.
**Example from PR #19:** FloatSampleList uses `double[] values` and `long[] timestamps` instead of `List<Sample>`
**How to Check:**
- Analyze access patterns
- Consider memory layout
- Benchmark different approaches
- Document trade-offs

#### Rule 5.3: Document Performance Optimizations
**Priority:** MEDIUM
**Frequency:** 2 occurrences (PRs #5, #19, #37)
**Primary Reviewer:** Code authors

**Rule:** Document performance optimizations with TODOs, measurements, and rationale. Include before/after metrics when available.
**Rationale:** Helps future maintainers understand trade-offs and prevents premature de-optimization.
**Example from PR #37:** Performance improvement documented with metrics: ">20% improvement in CPU core utilization" with supporting graphs
**Example from PR #5:** TODO documents algorithmic improvement: "Replace O(N log N) full sort with O(N log K) heap"
**How to Check:**
- Include performance metrics in PR descriptions
- Document algorithmic complexity
- Add TODOs for future optimizations

#### Rule 5.4: Optimize Frequent Operations
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #37)
**Primary Reviewer:** varunbharadwaj

**Rule:** Profile production workloads to identify hot paths. Move frequent checks to periodic background jobs when appropriate.
**Rationale:** Reduces per-request overhead in high-throughput scenarios.
**Example from PR #37:** Moved `shouldPeriodicallyFlush` check (20-30% CPU) from per-request to periodic job (10s interval), achieving >20% CPU reduction
**How to Check:**
- Profile production workloads
- Identify operations called on every request
- Consider async/periodic alternatives
- Measure impact before/after

#### Rule 5.5: Use Appropriate Collection Sizing
**Priority:** MEDIUM
**Frequency:** Observed in codebase
**Primary Reviewer:** Code pattern

**Rule:** Pre-size collections when final size is known or can be estimated. Use ArrayUtil.grow() for dynamic growth.
**Rationale:** Reduces array copying during growth.
**Example from FloatSampleList.Builder:** `BUILDER_INITIAL_CAPACITY = 16` and uses ArrayUtil for growth
**How to Check:**
- Pre-size ArrayList with known capacity
- Use growth strategies from ArrayUtil
- Avoid repeated small growths

---

### 6. Standards & Conventions [6 rules]

#### Rule 6.1: Follow Industry Standards for Units
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #16)
**Primary Reviewer:** philiplhchan, karenyrx

**Rule:** Use standard unit notation (OTLP/UCUM conventions). For binary units, use `MiBy` (mebibytes) not `MB` (megabytes).
**Rationale:** Aligns with OpenTelemetry conventions and prevents confusion.
**Example from PR #16:** Changed from "MB" to "MiBy" following UCUM notation (https://ucum.org/ucum#para-curly)
**How to Check:**
- Follow OTLP semantic conventions
- Use UCUM notation for units
- Document unit choices in constants
- Be consistent across codebase

#### Rule 6.2: Choose Appropriate Metric Granularity
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #16)
**Primary Reviewer:** karenyrx, philiplhchan

**Rule:** Choose metric units based on meaningful range vs granularity trade-off. For circuit breaker on 128GB nodes, MiB is better than bytes.
**Rationale:** Histogram buckets have limited range. Higher-level units capture important data while losing irrelevant precision.
**Example from PR #16:** Discussion: "2M bytes is only 2MB, which is too small... we care more about being able to capture higher values than granularity of lower values"
**How to Check:**
- Consider max values in production
- Evaluate histogram bucket ranges
- Choose units that capture meaningful range
- Document rationale

#### Rule 6.3: Use Appropriate Comment Prefixes
**Priority:** LOW
**Frequency:** Observed pattern
**Primary Reviewer:** Code convention

**Rule:** Use standard comment prefixes: TODO for future work, NOTE for important caveats, FIXME for known issues.
**Rationale:** Makes comments scannable and actionable.
**Example from code:** "TODO: Optimize performance...", "NOTE: This implementation returns a view..."
**How to Check:**
- Use TODO for planned improvements
- Use NOTE for important warnings
- Use FIXME for bugs to address
- Make TODOs actionable

#### Rule 6.4: Consistent Naming Conventions
**Priority:** MEDIUM
**Frequency:** Observed pattern
**Primary Reviewer:** Code convention

**Rule:** Use descriptive names: Stages end in "Stage", Constants are UPPER_CASE, builder pattern for complex construction.
**Rationale:** Makes code self-documenting and predictable.
**Example:** `TopKStage`, `BUILDER_INITIAL_CAPACITY`, `FloatSampleList.Builder`
**How to Check:**
- Follow established naming patterns
- Use consistent suffixes (Stage, Factory, Builder)
- Make names self-explanatory

#### Rule 6.5: Document Deployment Procedures
**Priority:** MEDIUM
**Frequency:** 1 occurrence (PR #15)
**Primary Reviewer:** karenyrx

**Rule:** For changes affecting production settings or deployment, document safe rollout procedure in PR.
**Rationale:** Prevents outages and enables safe rollback.
**Example from PR #15:** Documented 3-step procedure for removing settings
**How to Check:**
- Document deployment steps in PR description
- Test in staging first
- Include rollback procedure

#### Rule 6.6: Professional Commit Messages
**Priority:** LOW
**Frequency:** Standard practice
**Primary Reviewer:** General expectation

**Rule:** Follow conventional commit format. End with Claude Code attribution only when appropriate.
**Rationale:** Maintains professional git history.
**Example:** Standard format includes description, rationale, attribution
**How to Check:**
- Use conventional commit format
- Explain "why" not just "what"
- Include co-authorship when applicable

---

### 7. Project-Specific Conventions [12 rules]

#### Rule 7.1: M3QL Function Implementation Pattern
**Priority:** HIGH
**Frequency:** Observed across multiple M3QL PRs
**Primary Reviewer:** Code pattern

**Rule:** M3QL functions require: Stage class, PlanNode, Constants registration, Factory updates, Visitor updates, comprehensive tests.
**Rationale:** Ensures complete integration with M3QL parser and execution engine.
**Example from PR #8:** ChangedStage includes: ChangedStage, ChangedPlanNode, Constants.java update, PipelineStageFactory, M3PlanNodeFactory, M3PlanVisitor, SourceBuilderVisitor
**How to Check:**
- Create Stage implementing appropriate interface
- Create PlanNode for query planning
- Register in Constants
- Update all factories and visitors
- Add tests for stage and plan node

#### Rule 7.2: Pipeline Stage Annotation Required
**Priority:** HIGH
**Frequency:** Standard pattern
**Primary Reviewer:** Code convention

**Rule:** All pipeline stages must have `@PipelineStageAnnotation(name = "stageName")`.
**Rationale:** Enables stage discovery and registration.
**Example:** `@PipelineStageAnnotation(name = "topK")`
**How to Check:**
- Add annotation to all stage classes
- Use consistent naming
- Match annotation name to function name

#### Rule 7.3: Sample Type Consistency
**Priority:** HIGH
**Frequency:** 1 occurrence (PR #19)
**Primary Reviewer:** Design decision

**Rule:** SampleList implementations should be homogeneous (single sample type) unless there's explicit need for mixed types.
**Rationale:** Simplifies interface and implementation.
**Example from PR #19:** Removed mixed sample type support, documented: "We should revisit if we see such a need"
**How to Check:**
- Document sample type expectations
- Validate type homogeneity if required
- Consider if mixed types are needed

#### Rule 7.4: OpenSearch Test Framework Usage
**Priority:** HIGH
**Frequency:** Project standard (from CLAUDE.md)
**Primary Reviewer:** Code convention

**Rule:** Tests must extend OpenSearchTestCase or its subclasses. Use framework methods for random data.
**Rationale:** Leverages OpenSearch test infrastructure.
**Example:** `extends OpenSearchTestCase`, use `randomAlphaOfLength()`, `randomInt()`, `expectThrows()`
**How to Check:**
- Extend appropriate test base class
- Use framework helper methods
- Follow OpenSearch testing patterns

#### Rule 7.5: Aggregator Implementation Pattern
**Priority:** HIGH
**Frequency:** Project standard
**Primary Reviewer:** Code convention

**Rule:** Aggregators must implement required interfaces and support both shard-level and coordinator-level execution.
**Rationale:** Enables distributed aggregation.
**Example:** TimeSeriesUnfoldAggregator (shard), TimeSeriesCoordinatorAggregator (coordinator)
**How to Check:**
- Implement appropriate aggregator interfaces
- Support reduce() for coordinator aggregation
- Test both execution paths

#### Rule 7.6: Constant Organization
**Priority:** MEDIUM
**Frequency:** Observed pattern
**Primary Reviewer:** Code convention

**Rule:** Constants should be organized in appropriate constants classes (Constants.java, TSDBMetricsConstants.java).
**Rationale:** Centralized configuration.
**Example:** Function names in Constants.java, metric units in TSDBMetricsConstants.java
**How to Check:**
- Add constants to appropriate class
- Use descriptive names
- Include javadoc

#### Rule 7.7: Serialization Pattern for Pipeline Stages
**Priority:** HIGH
**Frequency:** Standard pattern
**Primary Reviewer:** Code convention

**Rule:** Pipeline stages must implement writeTo/StreamInput constructor for distributed execution.
**Rationale:** Enables sending stages across network for shard execution.
**Example pattern:**
```java
public void writeTo(StreamOutput out) throws IOException {
    out.writeInt(limit);
    out.writeEnum(mode);
}
public SliceStage(StreamInput in) throws IOException {
    this(in.readInt(), in.readEnum(HeadTailMode.class));
}
```
**How to Check:**
- Implement writeTo for all fields
- Implement StreamInput constructor
- Test serialization round-trip

#### Rule 7.8: XContent Serialization for Aggregations
**Priority:** HIGH
**Frequency:** Standard pattern
**Primary Reviewer:** Code convention

**Rule:** Aggregations must implement toXContent for JSON serialization of results.
**Rationale:** Enables REST API response formatting.
**Example:** Implement `toXContent(XContentBuilder builder, ToXContent.Params params)`
**How to Check:**
- Implement toXContent method
- Return all relevant fields
- Follow OpenSearch JSON conventions

#### Rule 7.9: Gradle Build Conventions
**Priority:** MEDIUM
**Frequency:** Project standard
**Primary Reviewer:** Code convention

**Rule:** Use appropriate gradle tasks: `precommit` for formatting, `check` for tests, `run` for local cluster.
**Rationale:** Consistent development workflow.
**Example from CLAUDE.md:**
- `./gradlew precommit` - Verify formatting
- `./gradlew check` - Run all tests
- `./gradlew run` - Start cluster
**How to Check:**
- Run precommit before committing
- Ensure check passes
- Test with local cluster

#### Rule 7.10: Code Formatting
**Priority:** MEDIUM
**Frequency:** Enforced by precommit
**Primary Reviewer:** Automated (spotless)

**Rule:** All code must pass `./gradlew spotlessApply` before commit.
**Rationale:** Consistent code style.
**Example:** Run spotlessApply to auto-format
**How to Check:**
- Run spotlessApply before committing
- Fix any formatting violations
- Configure IDE to match spotless

#### Rule 7.11: JavaCC Grammar Changes
**Priority:** HIGH
**Frequency:** Project standard
**Primary Reviewer:** Code convention

**Rule:** After modifying .jj grammar files, regenerate parser with `./gradlew generateJavaCC`.
**Rationale:** Keeps generated parser in sync with grammar.
**Example from CLAUDE.md:** Grammar in src/main/java/org/opensearch/tsdb/lang/m3/m3ql/parser/
**How to Check:**
- Run generateJavaCC after grammar changes
- Commit generated files
- Test parser with new grammar

#### Rule 7.12: Integration Test Patterns
**Priority:** HIGH
**Frequency:** Project standard
**Primary Reviewer:** Code convention

**Rule:** Use YAML-based test framework for M3QL integration tests. Tests in src/*/resources/test_cases/.
**Rationale:** Declarative testing of query language features.
**Example from PR #35:** Added alias validation to ExpectedData model for YAML tests
**How to Check:**
- Add YAML test cases for new features
- Follow existing YAML structure
- Test expected vs actual results

---

## philiplhchan's Top Priorities

Based on comment frequency and emphasis, philiplhchan focuses on:

1. **Architecture & Design** (highest priority)
   - Coordinator-only vs pushdown semantics
   - Reduce() behavior and ordering guarantees
   - Stage execution paths and distributed behavior
   - Future extensibility (data source specific logic)

2. **Performance & Optimization**
   - Identifying optimization opportunities (ConstantList usage in CountStage)
   - Avoiding unnecessary allocations (toList() in hot paths)
   - Documenting algorithmic improvements

3. **API Design & Documentation**
   - Clear method naming matching behavior (binary search → search)
   - Comprehensive documentation of edge cases
   - Input validation trade-offs

4. **Code Correctness**
   - Input invariant validation (when appropriate)
   - Mixed type support limitations
   - Mutable vs immutable object semantics

5. **Standards Compliance**
   - Following industry conventions (OTLP/UCUM for units)
   - Safe deployment procedures for breaking changes

---

## Common Review Phrases

### Blocking (CRITICAL priority)
- "this will break" - indicates production breaking change
- "we should..." (when about architecture) - strong suggestion
- "do we need to validate..." - question about correctness

### Important (HIGH priority)
- "should we..." - suggestion for improvement
- "can you add..." - request for additional work
- "Probably better follow this" - points to better pattern
- "[links to pattern]" - directive to follow existing approach

### Suggestions (MEDIUM priority)
- "I think..." - opinion/suggestion
- "maybe..." - optional improvement
- "btw..." - related observation
- "@reviewer is this a problem?" - seeking input

### Informational (LOW priority)
- "nit:" - minor style/preference
- "FYI" - information sharing
- Comments asking clarifying questions

---

## Recommended Agent Enhancements

### 1. Pre-Review Checklist
Add to agent prompt:
```
Before reviewing code, check:
□ Immutable collection usage (no modifications to List.of(), Collections.unmodifiable*)
□ Pipeline stage semantics (coordinator-only vs pushdown, reduce() vs process())
□ Input validation appropriateness (constructor args, null checks)
□ Performance considerations (avoid toList() in production, use SampleList directly)
□ Method names match behavior (especially after refactoring)
□ Documentation matches implementation (especially edge cases)
□ Tests cover new functionality (not just documentation)
□ Serialization support for distributed classes
□ Follow existing patterns (search for similar code first)
```

### 2. Category-Specific Checks

**For Pipeline Stages:**
```
□ Has @PipelineStageAnnotation
□ Implements isCoordinatorOnly() correctly
□ Implements isGlobalAggregation() correctly
□ Implements reduce() if not coordinator-only
□ Documents reduce() ordering semantics
□ Documents when process() vs reduce() is called
□ Handles empty input
□ Validates constructor arguments
□ Implements StreamInput/StreamOutput
□ Tests serialization
□ Updates Constants.java, factories, visitors
```

**For Data Structures:**
```
□ Efficient for access pattern
□ Minimizes allocations
□ Documents iterator view semantics if applicable
□ Validates invariants appropriately
□ Uses assertions for internal checks
□ Tests edge cases (empty, single element, null)
```

**For Settings Changes:**
```
□ Documents deployment procedure
□ Tests in staging
□ Handles archived settings gracefully
□ Includes rollback plan
```

### 3. Severity Classification

Add to agent prompt:
```
Classify issues by severity:

CRITICAL (block merge):
- Immutable collection modifications
- Broken distributed semantics (reduce/process confusion)
- Missing input validation on constructors
- Production breaking changes without migration path
- Coverage < 85%

HIGH (request changes):
- Missing tests for new functionality
- Performance issues in hot paths (toList() in production code)
- Inconsistent naming after refactoring
- Missing null checks
- Not following existing patterns

MEDIUM (suggest improvements):
- Missing edge case documentation
- Optimization opportunities
- Inconsistent unit naming
- Missing TODOs for future work

LOW (optional suggestions):
- Code style nits
- Minor documentation improvements
- Constant extraction
```

### 4. Auto-Reference Patterns

Add to agent knowledge base:
```
When reviewing X, reference these examples:
- New M3QL stage → PR #8 (ChangedStage), PR #5 (TopKStage), PR #6 (SliceStage)
- SampleList usage → PR #26 (RangeStage following AvgStage pattern)
- Pipeline stage tests → Existing stage test files
- Serialization → FloatSampleList, MinMaxSample patterns
- Performance optimization → PR #37 (periodic flush), PR #19 (FloatSampleList)
- Settings removal → PR #15 (safe deployment procedure)
- Unit conventions → PR #16 (UCUM/OTLP standards)
```

### 5. Question Templates

Add to agent:
```
Ask these questions during review:

Architecture:
- "Is this stage coordinator-only or does it support pushdown?"
- "How does reduce() behave when aggregations aren't ordered?"
- "Does this work correctly in distributed execution?"

Performance:
- "Could we use a more efficient data structure here?"
- "Is toList() necessary or can we use SampleList directly?"
- "Have we considered [optimization opportunity]?"

Correctness:
- "Do we need to validate [invariant] or is it guaranteed by API design?"
- "How do we handle null/empty input?"
- "What happens in edge case [X]?"

Patterns:
- "How do we do this elsewhere in the codebase?"
- "Does this follow the pattern in [similar class]?"
- "Should we add this to [existing test]?"
```

### 6. philiplhchan-Style Deep Dives

Agent should emulate philiplhchan's review style:
```
- Ask probing questions about distributed behavior
- Link to existing patterns when suggesting changes
- Identify cross-cutting optimization opportunities
- Question assumptions about ordering/guarantees
- Suggest future extensibility considerations
- Focus on "why" not just "what"
```

---

## Notes on Analysis Methodology

1. **Limited Sample Size:** Only 20 merged PRs analyzed. May not capture all team patterns.
2. **Reviewer Bias:** philiplhchan is most active reviewer, so patterns heavily reflect his priorities.
3. **Recent Timeframe:** All PRs from past 2 weeks, may not represent long-term patterns.
4. **Missing Context:** Some review discussions may have happened offline or in earlier commits.

## Recommended Next Steps

1. **Validate Rules:** Review with team to confirm accuracy and priority
2. **Add Examples:** Expand with more code examples from codebase
3. **Create Checklist:** Convert to PR template or review checklist
4. **Update Agent:** Incorporate rules into code-reviewer agent prompt
5. **Continuous Learning:** Update rules as team standards evolve
6. **Metrics:** Track how well agent catches issues vs human reviewers

---

## Appendix: Coverage by PR

| PR # | Title | Primary Patterns |
|------|-------|-----------------|
| 48 | Avoid modifying immutable collections | Architecture (1.1) |
| 38 | Remove duplicate settings | Code quality |
| 37 | Periodic flush optimization | Performance (5.4) |
| 35 | Alias checking for e2e tests | Testing (4.1), M3QL patterns (7.12) |
| 27 | Update to gradle 9.2 and JDK 25 | Build configuration |
| 26 | Add range aggregation | Performance (1.8), M3QL patterns (7.1), Validation (3.1) |
| 25 | Add observability in CCM | Observability |
| 21 | Fix ooo chunk metric | Bug fix |
| 20 | Make step size required | Architecture (1.4), Correctness (3.1) |
| 19 | FloatSampleList implementation | Performance (5.1, 5.2), Architecture (1.6), Code quality (2.1) |
| 17 | Update retention boundary logic | Bug fix, Code quality |
| 16 | Use MB for CB histogram | Standards (6.1, 6.2) |
| 15 | Remove CB warn log | Correctness (3.7), Standards (6.5) |
| 8 | M3QL changed stage | M3QL patterns (7.1), Architecture (1.3) |
| 6 | M3QL tail function | Architecture (1.2, 1.7), M3QL patterns (7.1) |
| 5 | TopK function | Architecture (1.2), M3QL patterns (7.1), Performance (5.3) |
| 4 | M3QL mapper functions | M3QL patterns (7.1) |
| 3 | Union function syntax | M3QL patterns (7.1, 7.11) |

---

*Generated: 2026-02-10*
*Analyzer: Claude (Sonnet 4.5)*
*Repository: opensearch-project/time-series-db*
