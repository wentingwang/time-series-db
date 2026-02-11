# Unit Testing Guide for OpenSearch TSDB Plugin

This skill provides comprehensive testing guidance for the OpenSearch TSDB plugin.

## Table of Contents

### [1. Architecture Overview](1-architecture.md)
Learn about the project structure and major components:
- Core Storage Engine
- M3QL Query Language
- Query Execution
- Project structure and key concepts

**When to read:** Getting familiar with the codebase or understanding component relationships.

---

### [2. Testing Commands Reference](2-testing-commands.md)
Quick reference for running tests:
- Full test suite commands
- Running specific tests
- M3QL pipeline tests
- Debugging and troubleshooting
- Build and development commands

**When to read:** Need to run tests or troubleshoot test failures.

---

### [3. M3QL Pipeline Stages Testing Guide](3-m3ql-pipeline-stages.md)
Detailed guide for testing M3QL query pipeline stages:
- Pipeline stage tests (20+ tests)
- Plan node tests (19+ tests)
- SourceBuilderVisitor integration (4 tests)
- End-to-end M3QL tests (4 files)
- Complete test coverage checklist

**When to read:** Adding a new M3QL pipeline stage (like `excludeByTag`, `perSecond`, `sum`, etc.)

**Scope:** `src/main/java/org/opensearch/tsdb/lang/m3/stage/` and related query pipeline components.

---

### [4. Serialization & Deserialization Testing Guide](4-serialization-testing.md)
Critical patterns for testing JSON serialization/deserialization:
- Round-trip testing (serialize → parse → deserialize)
- XContent parser testing (boolean, string, number, array handling)
- Enum serialization pitfalls (uppercase vs lowercase)
- Real-world complex examples
- Common bugs and how to catch them

**When to read:**
- Writing tests for classes with `toXContent()` / `fromArgs()` / `parse()` methods
- Testing aggregation builders or pipeline stages
- Debugging serialization/deserialization bugs

**Scope:** Any class that serializes to/from JSON (stages, aggregation builders, etc.)

**Critical:** Always include round-trip tests! Tests that only check JSON output can miss serious bugs.

---

## Quick Start Guide

### For Adding a New M3QL Pipeline Stage

1. **Read the Architecture** ([1-architecture.md](1-architecture.md))
   - Understand the M3QL pipeline: Query → Parser → Planner → Translator → Execution

2. **Follow the Testing Guides**
   - Main: [3-m3ql-pipeline-stages.md](3-m3ql-pipeline-stages.md) - Create stage tests (~20), plan node tests (~19), visitor tests (~4), end-to-end tests (4 files)
   - Serialization: [4-serialization-testing.md](4-serialization-testing.md) - Add round-trip tests for `toXContent()` / `fromArgs()`

3. **Use the Commands Reference** ([2-testing-commands.md](2-testing-commands.md))
   - Run your tests: `./gradlew test --tests YourStageTests --offline`
   - Fix formatting: `./gradlew spotlessApply`
   - Run full check: `./gradlew check`

### For Testing Serialization/Deserialization

1. **Read the Serialization Guide** ([4-serialization-testing.md](4-serialization-testing.md))
   - Understand round-trip testing pattern
   - Learn common pitfalls (enum case, boolean tokens, etc.)

2. **Write Critical Tests**
   - Round-trip test for all variants
   - XContent parser test with all token types
   - Real-world complex example

3. **Verify**
   - Run tests: `./gradlew test --tests YourTests --offline`
   - Ensure serialized JSON can be parsed back

### Test Coverage Summary

For a complete M3QL pipeline stage implementation:
- **~48 total tests** covering all aspects
- **4 test layers:** Stage → Plan Node → Visitor → End-to-End
- **4 files per end-to-end test:** .m3ql, ast, plan, dsl

---

## Component Testing Scope

| Component | Testing Guide | When to Use |
|-----------|---------------|-------------|
| M3QL Pipeline Stages | [3-m3ql-pipeline-stages.md](3-m3ql-pipeline-stages.md) | Adding `excludeByTag`, `perSecond`, `sum`, etc. |
| Serialization/Deserialization | [4-serialization-testing.md](4-serialization-testing.md) | Testing `toXContent()`, `fromArgs()`, `parse()` |
| Aggregation Builders | [4-serialization-testing.md](4-serialization-testing.md) | Testing `TimeSeriesCoordinatorAggregationBuilder`, etc. |
| Core Storage | *Not covered* | Different testing patterns |
| REST APIs | *Not covered* | Different testing patterns |

**Note:** This skill focuses on M3QL pipeline stages and serialization testing. Other components have different testing requirements.

---

## Example Implementation

**Reference:** ExcludeByTag implementation
- Location: `src/main/java/org/opensearch/tsdb/lang/m3/stage/ExcludeByTagStage.java`
- Tests: 48 total (20 stage + 19 plan node + 4 visitor + 4 end-to-end + 1 integration)
- All tests passing ✅

See [3-m3ql-pipeline-stages.md](3-m3ql-pipeline-stages.md) for detailed patterns used in this implementation.

---

## Contributing

Before committing:
1. Run `./gradlew spotlessApply` to fix formatting
2. Run `./gradlew check` to verify all tests pass
3. Ensure your tests follow the patterns in this guide

See [2-testing-commands.md](2-testing-commands.md) for all available commands.
