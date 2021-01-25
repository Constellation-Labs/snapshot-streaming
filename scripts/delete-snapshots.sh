#!/usr/bin/env bash

# Usage:
# 1. Set env variable:
#   export ES_BASE_URL="http://elasticsearch.com:9200"
# 2. Run script and pass snapshot ids (hashes) as positional arguments:
#   ./delete-snapshots.sh abc xyz

_ES_BASE_URL="${ES_BASE_URL:-http://localhost:9200}"

snapshots=("$@")

checkpoints=()
for snapshot in ${snapshots[*]}
do
 curr=( $(curl --silent "$_ES_BASE_URL/snapshots/_doc/$snapshot" | jq --raw-output '._source.checkpointBlocks[]') )
 checkpoints=("${checkpoints[@]}" "${curr[@]}")
done

transactions=()
for checkpoint in ${checkpoints[*]}
do
  curr=( $(curl --silent "$_ES_BASE_URL/checkpoints/_doc/$checkpoint" | jq --raw-output '._source.transactions[]') )
  transactions=("${transactions[@]}" "${curr[@]}")
done


bulkJsonStr=""
bulkJsonStr+=$(jq --null-input --compact-output '$ARGS.positional[] | {delete:{_index:"transactions",_id:.}}' --args "${transactions[@]}")$'\n'
bulkJsonStr+=$(jq --null-input --compact-output '$ARGS.positional[] | {delete:{_index:"checkpoints",_id:.}}' --args "${checkpoints[@]}")$'\n'
bulkJsonStr+=$(jq --null-input --compact-output '$ARGS.positional[] | {delete:{_index:"snapshots",_id:.}}' --args "${snapshots[@]}")$'\n'

echo "Documents selected for deletion"
echo "$bulkJsonStr"

read -r -p "Are you sure you want to delete these documents? [y/N] " response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]
then
    result=$(curl --silent -X POST "$_ES_BASE_URL/_bulk" -H "Content-Type: application/json" -d "$bulkJsonStr")
    echo "Deletion result"
    echo "$result"
else
    echo "Canceled"
fi
