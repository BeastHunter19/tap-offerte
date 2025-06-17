#!/bin/sh
set -e

ES_HOST=${ES_HOST:-http://elasticsearch:9200}

for index_file in /elasticsearch/index.*.json; do
  index_name=$(basename "$index_file" | sed 's/^index\.//' | sed 's/\.json$//')

  echo "Checking if index '$index_name' exists..."

  if curl -s -f -o /dev/null "$ES_HOST/$index_name"; then
    echo "Index '$index_name' already exists. Skipping."
  else
    echo "Creating index '$index_name' from file '$index_file'..."
    curl -X PUT "$ES_HOST/$index_name" \
      -H "Content-Type: application/json" \
      -d @"$index_file"
  fi
done
