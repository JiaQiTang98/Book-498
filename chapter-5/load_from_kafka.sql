CREATE TABLE test_topic (
    event_time DateTime,
    event_type String,
    event_data String
) ENGINE = Kafka()
settings kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'test_topic',
    kafka_group_name = 'clickhouse-group',
    kafka_format = 'JSONEachRow';

CREATE TABLE test_topic_parsed (
    event_time DateTime,
    event_type String,
    event_data String
) ENGINE = MergeTree()
ORDER BY event_time;

CREATE MATERIALIZED VIEW test_topic_parsed_mv TO test_topic_parsed AS
SELECT
    event_time,
    event_type,
    event_data
FROM test_topic;
