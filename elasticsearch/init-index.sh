#!/bin/sh

set -e

ES_HOST=${ES_HOST:-http://elasticsearch:9200}
INDEX_NAME=${INDEX_NAME:-offers}

if curl -s -f -o /dev/null "$ES_HOST/$INDEX_NAME"; then
  echo "Index '$INDEX_NAME' already exists. Skipping creation."
else
  echo "Creating index '$INDEX_NAME'..."
  curl -X PUT "$ES_HOST/$INDEX_NAME" \
    -H "Content-Type: application/json" \
    -d @/index.json
fi
