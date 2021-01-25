#!/usr/bin/env bash
set -e

# Get the docs with:
# ./delete-snapshots --help

_ES_BASE_URL="${ES_BASE_URL:-http://localhost:9200}"
_ES_SNAPSHOTS_INDEX="${ES_SNAPSHOTS_INDEX:-snapshots}"
_ES_CHECKPOINTS_INDEX="${ES_CHECKPOINTS_INDEX:-checkpoints}"
_ES_TRANSACTIONS_INDEX="${ES_TRANSACTIONS_INDEX:-transactions}"

curl_opts=(--silent --fail)

function error() {
  echo "$1" 1>&2
  exit 1
}

function run() {

  snapshots=("$@")

  checkpoints=()
  for snapshot in ${snapshots[*]}; do
    curr=()
    while IFS='' read -r line; do curr+=("$line"); done < <(curl "${curl_opts[@]}" "$_ES_BASE_URL/$_ES_SNAPSHOTS_INDEX/_doc/$snapshot" | jq --raw-output '._source.checkpointBlocks[]')
    checkpoints=("${checkpoints[@]}" "${curr[@]}")
  done

  transactions=()
  for checkpoint in ${checkpoints[*]}; do
    curr=()
    while IFS='' read -r line; do curr+=("$line"); done < <(curl "${curl_opts[@]}" "$_ES_BASE_URL/$_ES_CHECKPOINTS_INDEX/_doc/$checkpoint" | jq --raw-output '._source.transactions[]')
    transactions=("${transactions[@]}" "${curr[@]}")
  done

  function deleteItems() {
    jq --null-input --compact-output '$ids | split(" ")[] | {delete:{_index:$index,_id:.}}' --arg index "$1" --arg ids "$2"
  }

  bulkJsonStr=""
  bulkJsonStr+=$(deleteItems "$_ES_TRANSACTIONS_INDEX" "${transactions[*]}")$'\n'
  bulkJsonStr+=$(deleteItems "$_ES_CHECKPOINTS_INDEX" "${checkpoints[*]}")$'\n'
  bulkJsonStr+=$(deleteItems "$_ES_SNAPSHOTS_INDEX" "${snapshots[*]}")$'\n'

  echo "Documents selected for deletion"
  echo "$bulkJsonStr"

  read -r -p "Are you sure you want to delete these documents? [y/N] " response
  if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    result=$(curl "${curl_opts[@]}" -X POST "$_ES_BASE_URL/_bulk" -H "Content-Type: application/json" -d "$bulkJsonStr")
    echo "Deletion result"
    echo "$result"
  else
    echo "Canceled"
  fi
}

function validateInput() {

  curl "${curl_opts[@]}" "$_ES_BASE_URL" > /dev/null || error "Unable to connect to: $_ES_BASE_URL"

  function validateIndex() {
    curl "${curl_opts[@]}" "$_ES_BASE_URL/_cat/indices/$1" >/dev/null || error "Index not found: $1"
  }

  validateIndex "$_ES_SNAPSHOTS_INDEX"
  validateIndex "$_ES_CHECKPOINTS_INDEX"
  validateIndex "$_ES_TRANSACTIONS_INDEX"
}

positional=()
while [[ $# -gt 0 ]]; do
  arg="$1"

  case $arg in
  -b | --base-url)
    shift
    _ES_BASE_URL=$1
    shift
    ;;
  -s | --snapshots-index)
    shift
    _ES_SNAPSHOTS_INDEX=$1
    shift
    ;;
  -c | --checkpoints-index)
    shift
    _ES_CHECKPOINTS_INDEX=$1
    shift
    ;;
  -t | --transactions-index)
    shift
    _ES_TRANSACTIONS_INDEX=$1
    shift
    ;;
  -h | --help)
    cat <<-EOF
Usage ./$(basename "$0") [options] <snapshot hashes...>

Options:
  -b, --base-url <url>                Address of the elasticsearch REST API
  -s, --snapshots-index <name>        Name of the index containing snapshots (default "snapshots")
  -c, --checkpoints-index <name>      Name of the index containing checkpoint blocks (default "checkpoints")
  -t, --transactions-index <name>     Name of the index containing transactions (default "transactions")
EOF
    exit 0
    ;;
  *)
    positional+=("$1")
    shift
    ;;
  esac
done

set -- "${positional[@]}"

validateInput

run "$@"
