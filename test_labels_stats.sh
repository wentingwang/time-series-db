#!/bin/bash
#
# Quick test script for LabelsStatsAggregator
#
# Prerequisites: OpenSearch cluster must be running
#   ./gradlew run -PnumNodes=1
#

set -e

echo "=== Testing LabelsStatsAggregator ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base URL
BASE_URL="localhost:9200"

echo -e "${BLUE}Step 1: Creating TSDB index...${NC}"
curl -s -X PUT "$BASE_URL/test-metrics" -H "Content-Type: application/json" -d '{
  "settings": {
    "index": {
      "tsdb_engine.enabled": true,
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}' | jq -r '.acknowledged // .error'
echo ""

echo -e "${BLUE}Step 2: Indexing test data...${NC}"
# Index 5 documents with different label combinations
curl -s -X POST "$BASE_URL/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region us-west env prod",
  "timestamp": 1000000,
  "value": 42.5
}' | jq -r '._id'

curl -s -X POST "$BASE_URL/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region us-east env prod",
  "timestamp": 1001000,
  "value": 38.2
}' | jq -r '._id'

curl -s -X POST "$BASE_URL/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service web region us-west env prod",
  "timestamp": 1002000,
  "value": 55.7
}' | jq -r '._id'

curl -s -X POST "$BASE_URL/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service web region us-east env staging",
  "timestamp": 1003000,
  "value": 22.1
}' | jq -r '._id'

curl -s -X POST "$BASE_URL/test-metrics/_doc?refresh=true" -H "Content-Type: application/json" -d '{
  "labels": "service api region eu-west env prod",
  "timestamp": 1004000,
  "value": 67.3
}' | jq -r '._id'

echo -e "${GREEN}✓ Indexed 5 documents${NC}"
echo ""

echo -e "${BLUE}Step 3: Query all services...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:*&start=0&end=2000000&partitions=test-metrics" | jq
echo ""

echo -e "${BLUE}Step 4: Query only 'api' service...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:api&start=0&end=2000000&partitions=test-metrics" | jq
echo ""

echo -e "${BLUE}Step 5: Wildcard query for US regions...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20region:us-*&start=0&end=2000000&partitions=test-metrics" | jq
echo ""

echo -e "${BLUE}Step 6: Query with includeValueStats=false...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:*&start=0&end=2000000&includeValueStats=false&partitions=test-metrics" | jq
echo ""

echo -e "${BLUE}Step 7: Test error handling (missing query)...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?start=0&end=2000000" | jq
echo ""

echo -e "${BLUE}Step 8: Test error handling (invalid time range)...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:api&start=2000000&end=0" | jq
echo ""

echo -e "${BLUE}Step 9: Test explain mode (view translated DSL)...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:api&start=0&end=2000000&explain=true" | jq
echo ""

echo -e "${BLUE}Step 10: Pretty-print the translated DSL...${NC}"
curl -s -X GET "$BASE_URL/_labels_stats?query=fetch%20service:api%20region:us-*&start=0&end=2000000&explain=true" | jq -r '.translated_dsl' | jq
echo ""

echo -e "${GREEN}=== All tests completed! ===${NC}"
echo ""
echo -e "${BLUE}Cleanup (optional):${NC}"
echo "  curl -X DELETE \"$BASE_URL/test-metrics\""
