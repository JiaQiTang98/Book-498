#!/bin/bash

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS default.sharding_key_test_local ON CLUSTER default SYNC;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS default.sharding_key_test ON CLUSTER default SYNC;"

$CLICKHOUSE_CLIENT --query='
create table default.sharding_key_test_local ON CLUSTER default (
    `id` Int64,
    `name` String
)
ENGINE = ReplicatedMergeTree() 
ORDER BY tuple();'

$CLICKHOUSE_CLIENT --query='
create table default.sharding_key_test ON CLUSTER default (
    `id` Int64,
    `name` String
)
ENGINE = Distributed(default, default, sharding_key_test_local, id);'

MOD_EVEN=0
MOD_ODD=1
if [ $# -eq 1 ] && [ "$1" = "bad" ]; then
    MOD_EVEN=1
    MOD_ODD=0
fi

$CLICKHOUSE_CLIENT --port 9000 --query="INSERT INTO default.sharding_key_test_local (id, name) select number, toString(number) from numbers(1000000) where number%2=$MOD_EVEN;"
$CLICKHOUSE_CLIENT --port 9002 --query="INSERT INTO default.sharding_key_test_local (id, name) select number, toString(number) from numbers(1000000) where number%2=$MOD_ODD;"
