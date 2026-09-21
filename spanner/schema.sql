CREATE TABLE Events (
  topic STRING(MAX) NOT NULL,
  stream_name STRING(MAX) NOT NULL,
  version INT64 NOT NULL,
  global_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  partition_id INT64 NOT NULL,
  org_id STRING(MAX) NOT NULL,
  correlation_id STRING(MAX) NOT NULL,
  event_type STRING(MAX) NOT NULL,
  event_data BYTES(MAX) NOT NULL
) PRIMARY KEY (topic, stream_name, version);
CREATE INDEX Events_by_topic ON Events(topic, partition_id, global_timestamp, stream_name, version);
CREATE INDEX Events_by_org ON Events(topic, org_id, partition_id, global_timestamp, stream_name, version);
CREATE TABLE Topics (
  topic_name STRING(MAX) NOT NULL,
  partition_count INT64 NOT NULL
) PRIMARY KEY (topic_name);
CREATE TABLE Cursors (
  topic STRING(MAX) NOT NULL,
  subscriber_id STRING(MAX) NOT NULL,
  partition_id INT64 NOT NULL,
  cursor STRING(MAX) NOT NULL,
  updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (topic, subscriber_id, partition_id);
