DROP TABLE IF EXISTS default.sharding_key_test_local ON CLUSTER default SYNC;
DROP TABLE IF EXISTS default.sharding_key_test ON CLUSTER default SYNC;

create table default.sharding_key_test_local ON CLUSTER default (
    `id` Int64,
    `name` String
)
ENGINE = ReplicatedMergeTree() 
ORDER BY tuple();

create table default.sharding_key_test ON CLUSTER default (
    `id` Int64,
    `name` String
)
ENGINE = Distributed(default, default, sharding_key_test_local, id);

INSERT INTO default.sharding_key_test (id, name) select number, toString(number) from numbers(1000000);
