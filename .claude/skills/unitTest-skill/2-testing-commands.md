# Testing Commands Reference

## Quick Reference

### Full Test Suite
```bash
# Run all tests, formatting, coverage, and precommit checks
./gradlew check

# Run all tests without coverage verification
./gradlew test -x jacocoTestCoverageVerification --offline

# Run tests without downloading dependencies (faster)
./gradlew test --offline
```

### Code Formatting
```bash
# Fix all formatting issues
./gradlew spotlessApply

# Check formatting without fixing
./gradlew spotlessCheck

# Verify precommit checks (formatting, licenses, etc.)
./gradlew precommit
```

## Running Specific Tests

### Single Test Class
```bash
# Run a specific test class
./gradlew test --tests YourStageTests -x jacocoTestCoverageVerification --offline

# Example: Run ExcludeByTagStageTests
./gradlew test --tests ExcludeByTagStageTests -x jacocoTestCoverageVerification --offline
```

### Single Test Method
```bash
# Run a specific test method
./gradlew test --tests "YourStageTests.testSpecificMethod" --offline

# Example: Run specific test
./gradlew test --tests "ExcludeByTagStageTests.testExcludeTimeSeriesWithSingleMatchingPattern" --offline
```

### Multiple Test Classes
```bash
# Run multiple test classes
./gradlew test --tests YourStageTests --tests YourPlanNodeTests -x jacocoTestCoverageVerification --offline

# Example: Run all ExcludeByTag tests
./gradlew test --tests ExcludeByTagStageTests --tests ExcludeByTagPlanNodeTests -x jacocoTestCoverageVerification --offline
```

### Pattern Matching
```bash
# Run all tests matching a pattern
./gradlew test --tests "*ExcludeByTag*" -x jacocoTestCoverageVerification --offline

# Run specific end-to-end test by number
./gradlew test --tests "M3OSTranslatorTests.testM3OSTranslator*22*" -x jacocoTestCoverageVerification --offline
```

## M3QL Pipeline Tests

### Parser Tests (AST Generation)
```bash
# Run all parser tests
./gradlew test --tests M3ParserTests -x jacocoTestCoverageVerification --offline

# Run specific parser test
./gradlew test --tests "M3ParserTests.testASTGeneration*22*" --offline
```

### Planner Tests (Plan Generation)
```bash
# Run all planner tests
./gradlew test --tests M3PlannerTests -x jacocoTestCoverageVerification --offline

# Run specific planner test
./gradlew test --tests "M3PlannerTests.testPlanGeneration*22*" --offline
```

### Translator Tests (DSL Generation)
```bash
# Run all translator tests
./gradlew test --tests M3OSTranslatorTests -x jacocoTestCoverageVerification --offline

# Run specific translator test
./gradlew test --tests "M3OSTranslatorTests.testM3OSTranslator*22*" --offline
```

### End-to-End Pipeline Tests
```bash
# Run parser, planner, and translator tests
./gradlew test --tests M3ParserTests --tests M3PlannerTests --tests M3OSTranslatorTests -x jacocoTestCoverageVerification --offline
```

## Component-Specific Tests

### Stage Tests
```bash
# Run all stage tests in a directory
./gradlew test --tests "org.opensearch.tsdb.lang.m3.stage.*" -x jacocoTestCoverageVerification --offline

# Run specific stage test
./gradlew test --tests "org.opensearch.tsdb.lang.m3.stage.ExcludeByTagStageTests" --offline
```

### Plan Node Tests
```bash
# Run all plan node tests
./gradlew test --tests "org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.*" -x jacocoTestCoverageVerification --offline
```

### SourceBuilderVisitor Tests
```bash
# Run visitor tests
./gradlew test --tests SourceBuilderVisitorTests -x jacocoTestCoverageVerification --offline

# Run specific visitor test for a feature
./gradlew test --tests "SourceBuilderVisitorTests.testExcludeByTag*" --offline
```

## Test Output & Reports

### View Test Reports
```bash
# Test results location
open build/reports/tests/test/index.html

# Coverage report location
open build/reports/jacoco/test/html/index.html
```

### Test Result Files
```bash
# XML test results
ls build/test-results/test/*.xml

# Find test output
find build/testrun/test -name "*.log"
```

## Debugging Tests

### Run with Stack Trace
```bash
./gradlew test --tests YourStageTests --stacktrace --offline
```

### Run with Debug Output
```bash
./gradlew test --tests YourStageTests --debug --offline
```

### Run with Info Logging
```bash
./gradlew test --tests YourStageTests --info --offline
```

### Reproduce Specific Test Failure
When a test fails, Gradle provides a reproduce command:
```bash
# Copy from test output, example:
./gradlew ':test' --tests 'ExcludeByTagPlanNodeTests' \
  -Dtests.method='testExcludeByTagPlanNodeFactoryMethod' \
  -Dtests.seed=37733651B86634A3 \
  -Dtests.security.manager=true \
  -Dtests.locale=kkj-Latn-CM \
  -Dtests.timezone=Cuba \
  -Druntime.java=21
```

## Build & Development

### Clean Build
```bash
# Clean all build artifacts
./gradlew clean

# Clean and rebuild
./gradlew clean build
```

### Compile Only
```bash
# Compile main code
./gradlew compileJava

# Compile test code
./gradlew compileTestJava
```

### Run Local Cluster
```bash
# Start single node
./gradlew run

# Start multi-node cluster
./gradlew run -PnumNodes=2
```

### Benchmarks
```bash
# Run JMH benchmarks
./gradlew jmh

# Run specific benchmark
./gradlew jmh -Pinclude="TimeSeriesUnfoldAggregationBenchmark"
```

## Common Flags

### Offline Mode
```bash
--offline    # Don't download dependencies (faster, works offline)
```

### Skip Tasks
```bash
-x jacocoTestCoverageVerification    # Skip coverage check
-x spotlessCheck                      # Skip formatting check
-x test                               # Skip tests entirely
```

### Test Selection
```bash
--tests TestClass                     # Run specific class
--tests "TestClass.testMethod"        # Run specific method
--tests "*Pattern*"                   # Pattern matching
```

## Troubleshooting

### Tests Failing Due to Formatting
```bash
# Fix formatting first
./gradlew spotlessApply

# Then run tests
./gradlew test
```

### Cached Test Results
```bash
# Force re-run all tests
./gradlew cleanTest test
```

### Dependency Issues
```bash
# Refresh dependencies
./gradlew --refresh-dependencies test

# View dependency tree
./gradlew dependencies
```

### Parser Changes
```bash
# If you modified m3ql.jj parser file
./gradlew generateJavaCC
./gradlew compileJava
```

## Best Practices

1. **Always run formatting before tests:**
   ```bash
   ./gradlew spotlessApply && ./gradlew check
   ```

2. **Use offline mode for faster iteration:**
   ```bash
   ./gradlew test --tests YourTests --offline
   ```

3. **Run full check before committing:**
   ```bash
   ./gradlew check
   ```

4. **Skip coverage for faster development:**
   ```bash
   ./gradlew test -x jacocoTestCoverageVerification --offline
   ```

5. **Run related tests together:**
   ```bash
   ./gradlew test --tests "*YourFeature*" -x jacocoTestCoverageVerification --offline
   ```
