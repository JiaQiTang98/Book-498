DROP TABLE IF EXISTS default.sharding_key_test_local ON CLUSTER default SYNC;
DROP TABLE IF EXISTS default.sharding_key_test ON CLUSTER default SYNC;

create table default.sharding_key_test_local ON CLUSTER default (
    `user_id` Int64,
    `name` String
)
ENGINE = ReplicatedMergeTree() 
ORDER BY tuple();

create table default.sharding_key_test ON CLUSTER default (
    `user_id` Int64,
    `name` String
)
ENGINE = Distributed(default, default, sharding_key_test_local, user_id);

INSERT INTO default.sharding_key_test (user_id, name) select number, toString(number) from numbers(300000000);
