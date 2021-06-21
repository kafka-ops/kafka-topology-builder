CREATE TABLE users ( id BIGINT PRIMARY KEY, usertimestamp BIGINT,  gender VARCHAR, region_id VARCHAR)
WITH (
    KAFKA_TOPIC = 'my-users-topic', 
    KEY_FORMAT='KAFKA', PARTITIONS=2, REPLICAS=1,
    VALUE_FORMAT = 'JSON'
);