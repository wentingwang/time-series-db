# Testing LabelsStatsAggregator Locally

This guide shows how to test the `/_labels_stats` endpoint with a local OpenSearch TSDB cluster.

## 1. Start OpenSearch Cluster

```bash
# Start a single-node cluster
./gradlew run -PnumNodes=1

# Or start a multi-node cluster
./gradlew run -PnumNodes=2
```

Wait for the cluster to start (you'll see "started" in the logs).

## 2. Create a TSDB Index

```bash
curl -X PUT "localhost:9200/test-metrics" -H "Content-Type: application/json" -d '{
  "settings": {
    "index": {
      "tsdb_engine.enabled": true,
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}'
```

## 3. Index Test Data

**Important**: Labels must be a **space-separated string** of key-value pairs:
- Format: `"key1 value1 key2 value2 key3 value3"`
- Example: `"service api region us-west env prod"`

### Index Sample Documents

```bash
# Document 1: service=api, region=us-west, env=prod
curl -X POST "localhost:9200/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region us-west env prod",
  "timestamp": 1000000,
  "value": 42.5
}'

# Document 2: service=api, region=us-east, env=prod
curl -X POST "localhost:9200/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region us-east env prod",
  "timestamp": 1001000,
  "value": 38.2
}'

# Document 3: service=web, region=us-west, env=prod
curl -X POST "localhost:9200/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service web region us-west env prod",
  "timestamp": 1002000,
  "value": 55.7
}'

# Document 4: service=web, region=us-east, env=staging
curl -X POST "localhost:9200/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service web region us-east env staging",
  "timestamp": 1003000,
  "value": 22.1
}'

# Document 5: service=api, region=eu-west, env=prod
curl -X POST "localhost:9200/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region eu-west env prod",
  "timestamp": 1004000,
  "value": 67.3
}'
```

## 4. Query Labels Stats

### Basic Query (all services)

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:*&start=0&end=2000000&partitions=test-metrics" | jq
```

Expected output:
```json
{
  "labels_stats": {
    "totalTimeSeries": 5,
    "tagStats": {
      "service": {
        "totalTimeSeries": 5,
        "values": ["api", "web"],
        "valuesStats": {
          "api": 3,
          "web": 2
        }
      },
      "region": {
        "totalTimeSeries": 5,
        "values": ["eu-west", "us-east", "us-west"],
        "valuesStats": {
          "us-west": 2,
          "us-east": 2,
          "eu-west": 1
        }
      },
      "env": {
        "totalTimeSeries": 5,
        "values": ["prod", "staging"],
        "valuesStats": {
          "prod": 4,
          "staging": 1
        }
      }
    }
  }
}
```

### Filter by Service

```bash
# Only API service
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api&start=0&end=2000000&partitions=test-metrics" | jq

# Only Web service
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:web&start=0&end=2000000&partitions=test-metrics" | jq
```

### Filter by Multiple Labels

```bash
# API service in production
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api%20env:prod&start=0&end=2000000&partitions=test-metrics" | jq
```

Expected: 2 time series (us-west and eu-west, but not us-east since we only indexed us-east for web)

### Wildcard Queries

```bash
# All US regions
curl -X GET "localhost:9200/_labels_stats?query=fetch%20region:us-*&start=0&end=2000000&partitions=test-metrics" | jq
```

Expected: 4 time series (us-west and us-east across api and web)

### Without Value Stats

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:*&start=0&end=2000000&includeValueStats=false&partitions=test-metrics" | jq
```

Expected output (no valuesStats field):
```json
{
  "labels_stats": {
    "totalTimeSeries": 5,
    "tagStats": {
      "service": {
        "totalTimeSeries": 5,
        "values": ["api", "web"]
      },
      "region": {
        "totalTimeSeries": 5,
        "values": ["eu-west", "us-east", "us-west"]
      },
      "env": {
        "totalTimeSeries": 5,
        "values": ["prod", "staging"]
      }
    }
  }
}
```

## 5. Error Cases

### Missing Query Parameter

```bash
curl -X GET "localhost:9200/_labels_stats?start=0&end=2000000" | jq
```

Expected:
```json
{
  "error": "Query parameter is required"
}
```

### Invalid Time Range

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api&start=2000000&end=0" | jq
```

Expected:
```json
{
  "error": "Start time must be before end time"
}
```

### Non-Fetch Query

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api%20%7C%20sum&start=0&end=2000000" | jq
```

Expected error (cannot use pipeline stages, only simple fetch)

## 6. Explain Mode (View Translated DSL)

The `explain` parameter shows you the translated OpenSearch DSL without executing the query:

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api&start=0&end=2000000&explain=true" | jq
```

Expected output:
```json
{
  "query": "fetch service:api",
  "translated_dsl": "{\"size\":0,\"query\":{\"time_range_pruning\":{\"query\":{\"bool\":{\"filter\":[{\"range\":{\"timestamp_range\":{\"from\":0,\"to\":2000000,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}},{\"terms\":{\"labels\":[\"service|api\"],\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"start\":0,\"end\":2000000}},\"aggregations\":{\"labels_stats\":{\"labels_stats\":{\"minTimestamp\":0,\"maxTimestamp\":2000000,\"includeValueStats\":true}}}}",
  "explanation": "M3QL fetch query translated to OpenSearch DSL with labels_stats aggregation"
}
```

You can also pretty-print the DSL using `jq`:

```bash
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:api%20region:us-*&start=0&end=2000000&explain=true" | jq -r '.translated_dsl' | jq
```

### More Explain Examples

```bash
# Simple fetch
curl "localhost:9200/_labels_stats?query=fetch%20service:api&start=0&end=2000000&explain=true" | jq

# Multiple labels
curl "localhost:9200/_labels_stats?query=fetch%20service:api%20env:prod&start=0&end=2000000&explain=true" | jq

# Wildcard query
curl "localhost:9200/_labels_stats?query=fetch%20region:us-*&start=0&end=2000000&explain=true" | jq

# Without value stats
curl "localhost:9200/_labels_stats?query=fetch%20service:*&start=0&end=2000000&includeValueStats=false&explain=true" | jq
```

## 8. Advanced Examples

### Time Range Filtering

```bash
# Only documents between timestamp 1000000 and 1002000
curl -X GET "localhost:9200/_labels_stats?query=fetch%20service:*&start=1000000&end=1002000&partitions=test-metrics" | jq
```

### POST Request

```bash
curl -X POST "localhost:9200/_labels_stats?query=fetch%20service:api&start=0&end=2000000&partitions=test-metrics" | jq
```

## 9. Cleanup

```bash
# Delete the test index
curl -X DELETE "localhost:9200/test-metrics"

# Stop the cluster
pkill -f "opensearch.*integTest"
```

## Notes

- **Labels Format**: Must be space-separated key-value pairs as a single string
  - ✅ Correct: `"service api region us-west env prod"`
  - ❌ Wrong: `["service:api", "region:us-west"]` (array)
  - ❌ Wrong: `{"service": "api", "region": "us-west"}` (object)

- **URL Encoding**: Remember to encode query parameters in URLs
  - Space → `%20`
  - Colon → `:`
  - Asterisk → `*`

- **Time Units**: Timestamps are in milliseconds since epoch

- **Default Parameters**:
  - `start`: `now-5m`
  - `end`: `now`
  - `includeValueStats`: `true`
  - `partitions`: all indices
