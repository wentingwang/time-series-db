# OpenSearch TSDB Plugin - Project Architecture Overview

## High-Level Architecture

The OpenSearch TSDB plugin provides time-series database capabilities for OpenSearch with M3QL query language support.

## Major Components

### 1. Core Storage Engine (`src/main/java/org/opensearch/tsdb/core/`)
Time-series storage with XOR compression.

**Key Subcomponents:**
- **Head** (`core/head/`) - In-memory active time series
- **ClosedChunkIndex** (`core/index/closed/`) - Disk-based compressed time series
- **Chunks & Compression** - XOR compression for efficient storage

**Testing Focus:** Storage, compression, indexing, data integrity

---

### 2. M3QL Query Language (`src/main/java/org/opensearch/tsdb/lang/m3/`)

#### Query Pipeline Architecture
```
M3QL Query → Parser (AST) → Planner (Plan Nodes) → Translator (DSL) → OpenSearch Query → Stage Execution
```

**Subcomponents:**

##### 2a. Parser (`lang/m3/m3ql/parser/`)
- **Technology:** JavaCC-based parser
- **Input:** M3QL query string
- **Output:** Abstract Syntax Tree (AST)
- **Regenerate:** `./gradlew generateJavaCC`

##### 2b. Planner (`lang/m3/m3ql/plan/`)
- **Input:** AST
- **Output:** Plan Node tree
- **Components:** Plan nodes (FetchPlanNode, AggregationPlanNode, etc.)

##### 2c. Translator (`lang/m3/dsl/`)
- **Input:** Plan Node tree
- **Output:** OpenSearch DSL (SearchSourceBuilder)
- **Key Class:** `M3OSTranslator`, `SourceBuilderVisitor`

##### 2d. Pipeline Stages (`lang/m3/stage/`)
- **Purpose:** Execute transformation/aggregation operations on time series
- **Examples:** `PerSecondStage`, `SumStage`, `MovingStage`, `ExcludeByTagStage`
- **Interface:** `UnaryPipelineStage` or `BinaryPipelineStage`
- **Location:** `src/main/java/org/opensearch/tsdb/lang/m3/stage/`

**Testing Focus:** Parser correctness, plan generation, DSL translation, stage execution

---

### 3. Query Execution (`src/main/java/org/opensearch/tsdb/query/`)

**Subcomponents:**

##### 3a. Aggregators (`query/aggregator/`)
- **TimeSeriesUnfoldAggregator** - Shard-level aggregation with pipeline stages
- **TimeSeriesCoordinatorAggregator** - Coordinator-level aggregation
- Integration with OpenSearch aggregation framework

##### 3b. REST APIs (`query/rest/`)
- HTTP endpoints for M3QL queries
- Request/response handling

##### 3c. Stage Execution (`query/stage/`)
- Runtime execution of pipeline stages
- `PipelineStageFactory` - Factory for creating stages from configuration

**Testing Focus:** Aggregation correctness, REST API contract, stage execution

---

## Project Structure Summary

```
src/main/java/org/opensearch/tsdb/
├── core/                    # Storage engine (chunks, indexes, compression)
│   ├── head/               # In-memory active time series
│   └── index/closed/       # Disk-based compressed time series
├── lang/m3/                # M3QL language implementation
│   ├── m3ql/
│   │   ├── parser/         # JavaCC parser → AST
│   │   └── plan/           # Planner → Plan Nodes
│   ├── dsl/                # Translator → OpenSearch DSL
│   └── stage/              # Pipeline stage implementations
└── query/                  # Query execution
    ├── aggregator/         # OpenSearch aggregators
    ├── rest/               # REST endpoints
    └── stage/              # Stage execution runtime
```

## Testing Structure

Tests mirror the source structure:
```
src/test/java/org/opensearch/tsdb/
src/test/resources/org/opensearch/tsdb/lang/m3/data/
    ├── queries/            # M3QL test queries
    ├── ast/                # Expected AST outputs
    ├── plan/               # Expected plan outputs
    └── dsl/                # Expected DSL outputs
```

## Common Tasks

- **Run cluster:** `./gradlew run -PnumNodes=2`
- **Run all tests:** `./gradlew check`
- **JMH benchmarks:** `./gradlew jmh`
- **Regenerate JavaCC parser:** `./gradlew generateJavaCC`
- **Format code:** `./gradlew spotlessApply`

## Key Concepts

### Pipeline Stages
- Operate on `List<TimeSeries>` inputs
- Support serialization for distributed execution
- Registered via `@PipelineStageAnnotation`
- Created via `PipelineStageFactory`

### Time Series Data Model
- **TimeSeries:** Collection of samples with labels and time range
- **Sample:** Timestamp + value pair
- **Labels:** Key-value pairs for metric identification
- **ByteLabels:** Efficient byte-based label storage

### M3QL Functions
M3QL functions map to pipeline stages:
- `fetch` → FetchPlanNode → Query building
- `sum` → AggregationPlanNode → SumStage
- `perSecond` → PerSecondPlanNode → PerSecondStage
- `excludeByTag` → ExcludeByTagPlanNode → ExcludeByTagStage

## References

- Build & Test: See `CLAUDE.md` in project root
- Benchmarks: `src/jmhTest/java/org/opensearch/search/aggregator/`
