# JMH Benchmark Skill

This skill provides a convenient interface for running JMH (Java Microbenchmark Harness) benchmarks in the OpenSearch TSDB project.

## Overview

The JMH skill simplifies running performance benchmarks with various configurations and options. It wraps the Gradle JMH tasks and provides an easy-to-use command-line interface.

## Installation

The skill is located at `.claude/skills/jmh-skill` and is executable. No additional installation is required.

## Usage

### Basic Syntax

```bash
.claude/skills/jmh-skill [COMMAND] [OPTIONS]
```

### Commands

#### `list`
List all available benchmarks in the project.

```bash
.claude/skills/jmh-skill list
```

#### `run [PATTERN]`
Run benchmarks matching the specified pattern. If no pattern is provided, all benchmarks are run.

```bash
# Run all benchmarks
.claude/skills/jmh-skill run

# Run specific benchmark
.claude/skills/jmh-skill run TermVsTermsBenchmark

# Run benchmarks matching pattern
.claude/skills/jmh-skill run TimeSeriesUnfold
```

#### `profilers`
List all available JMH profilers that can be used for performance analysis.

```bash
.claude/skills/jmh-skill profilers
```

#### `help`
Display help information.

```bash
.claude/skills/jmh-skill help
```

### Options

- `-w, --warmup N` - Number of warmup iterations (default: 3)
- `-i, --iterations N` - Number of measurement iterations (default: 5)
- `-f, --forks N` - Number of forks (default: 1)
- `-p, --params PARAMS` - Custom parameters (e.g., "cardinality=100")
- `-prof PROFILER` - Enable profiler (e.g., gc, stack)

## Examples

### Running Specific Benchmarks

```bash
# Run TermVsTermsBenchmark
.claude/skills/jmh-skill run TermVsTermsBenchmark

# Run HeadAppendBenchmark
.claude/skills/jmh-skill run HeadAppendBenchmark

# Run TimeSeriesUnfoldAggregationBenchmark
.claude/skills/jmh-skill run TimeSeriesUnfoldAggregationBenchmark
```

### Custom Iterations

```bash
# Run with 5 warmup iterations and 10 measurement iterations
.claude/skills/jmh-skill run -w 5 -i 10 TermVsTermsBenchmark

# Run with 2 forks
.claude/skills/jmh-skill run -f 2 HeadAppendBenchmark
```

### Using Profilers

```bash
# Run with GC profiler
.claude/skills/jmh-skill run -prof gc TimeSeriesUnfoldAggregationBenchmark

# Run with stack profiler
.claude/skills/jmh-skill run -prof stack TermVsTermsBenchmark

# Run with multiple profilers
.claude/skills/jmh-skill run -prof gc -prof stack TimeSeriesUnfoldAggregationBenchmark
```

### Custom Parameters

```bash
# Run with specific cardinality
.claude/skills/jmh-skill run -p "cardinality=1000" TimeSeriesUnfoldAggregationBenchmark

# Run with multiple parameters
.claude/skills/jmh-skill run -p "cardinality=1000,sampleCount=100" TimeSeriesUnfoldAggregationBenchmark
```

### Combined Options

```bash
# Run with custom iterations and profiler
.claude/skills/jmh-skill run -w 5 -i 10 -prof gc TermVsTermsBenchmark

# Run with custom parameters, iterations, and profiler
.claude/skills/jmh-skill run -p "cardinality=10000" -i 5 -prof gc TimeSeriesUnfoldAggregationBenchmark
```

## Available Benchmarks

The project currently includes the following benchmarks:

1. **TermVsTermsBenchmark** - Compares performance of `term` query vs `terms` query
   - `benchmarkTermQuerySingle` - Single term using term query
   - `benchmarkTermsQuerySingle` - Single term using terms query
   - `benchmarkTermsQueryMultiple` - Multiple terms using terms query

2. **TimeSeriesUnfoldAggregationBenchmark** - Benchmarks time series unfold aggregation
   - Parameters: cardinality, sampleCount, labelCount, stageType

3. **HeadAppendBenchmark** - Benchmarks appending data to the head chunk

4. **LabelsBenchmark** - Benchmarks label operations

5. **MergeIteratorBenchmark** - Benchmarks merge iterator performance

## Understanding Results

JMH results include:

- **Score** - Average time per operation (lower is better)
- **Error** - Margin of error
- **Units** - Time unit (e.g., microseconds, milliseconds)
- **Mode** - Benchmark mode (AverageTime, Throughput, etc.)

Example output:
```
Benchmark                                           Mode  Cnt  Score   Error  Units
TermVsTermsBenchmark.benchmarkTermQuerySingle       avgt   10  123.456 ± 5.678  us/op
TermVsTermsBenchmark.benchmarkTermsQuerySingle      avgt   10  145.678 ± 6.789  us/op
TermVsTermsBenchmark.benchmarkTermsQueryMultiple    avgt   10  234.567 ± 8.901  us/op
```

## Profilers

Common profilers include:

- **gc** - Garbage collection profiler
- **stack** - Stack profiler (sampling)
- **perf** - Linux perf profiler (Linux only)
- **perfnorm** - Normalized perf profiler (Linux only)
- **perfasm** - Assembly-level perf profiler (Linux only)
- **async** - Async-profiler (requires installation)

## Tips

1. **Warmup Iterations**: Ensure sufficient warmup to allow JIT compilation
2. **Measurement Iterations**: Use enough iterations for statistical significance
3. **Forks**: Multiple forks help reduce measurement bias
4. **Profilers**: Use profilers to understand performance bottlenecks
5. **Parameters**: Test with realistic parameter values

## Troubleshooting

### Benchmark Not Found

If a benchmark is not found, verify it's compiled:

```bash
./gradlew :benchmarks:classes
```

Then list available benchmarks:

```bash
.claude/skills/jmh-skill list
```

### Out of Memory

If you encounter OOM errors, adjust JVM heap size in the benchmark annotation:

```java
@Fork(value = 1, jvmArgsAppend = {"-Xms8g", "-Xmx8g"})
```

### Connection Errors (for client-based benchmarks)

For benchmarks that connect to OpenSearch (like TermVsTermsBenchmark), ensure:

1. OpenSearch is running: `./gradlew run`
2. The correct host and port are configured in the benchmark
3. The required index and data exist

## Direct Gradle Usage

You can also use Gradle directly if you prefer:

```bash
# Run all benchmarks
./gradlew jmh

# Run specific benchmark
./gradlew jmh -Pjmh.includes='.*TermVsTermsBenchmark.*'

# Run with options
./gradlew jmh \
  -Pjmh.includes='.*TermVsTermsBenchmark.*' \
  -Pjmh.warmupIterations=5 \
  -Pjmh.measurementIterations=10 \
  -Pjmh.profilers=gc
```

## References

- [JMH Documentation](https://github.com/openjdk/jmh)
- [JMH Samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples)
- [OpenSearch TSDB README](../README.md)

## Contributing

When adding new benchmarks:

1. Place them in `benchmarks/src/main/java/org/opensearch/tsdb/benchmark/`
2. Extend `BaseTSDBBenchmark` if needed
3. Use appropriate JMH annotations
4. Document parameters and setup requirements
5. Test with the JMH skill before committing
