#!/bin/bash
schemaregistry="$1"
topic="$2"
avscFile="$3"
tmpfile=$(mktemp)

export SCHEMA=$(cat "$avscFile")
echo '{"schema":""}' | jq --arg schema "$SCHEMA" '.schema = $schema' \
   > "$tmpfile"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @"$tmpfile" "${schemaregistry}"/subjects/"${topic}"-value/versions

#topic=json-topic
#export SCHEMA=$(cat schema.json)
#echo '{"schema":"","schemaType":"JSON"}' | jq --arg schema "$SCHEMA" '.schema = $schema' \
#   > $tmpfile
#curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#    --data @$tmpfile ${schemaregistry}/subjects/${topic}-value/versions
#rm $tmpfile