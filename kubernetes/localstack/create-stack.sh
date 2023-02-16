#!/usr/bin/env bash

awslocal s3api create-bucket --bucket snapshots
awslocal opensearch create-domain --domain-name block-explorer
