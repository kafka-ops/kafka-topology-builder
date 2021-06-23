package com.purbon.kafka.topology.model.users.platform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.purbon.kafka.topology.model.User;
import java.util.Optional;

public class SchemaRegistryInstance extends User {

  private static final String DEFAULT_SCHEMA_TOPIC = "_schemas";

  private static final String DEFAULT_CONSUMER_OFFSETS_TOPIC = "__consumer_offsets";

  private static final String DEFAULT_SCHEMA_REGISTRY_GROUP = "schema-registry";

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> topic;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> consumer_offsets_topic;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> group;

  public SchemaRegistryInstance() {
    this("");
  }

  public SchemaRegistryInstance(String principal) {
    this(principal, Optional.empty(), Optional.empty(), Optional.empty());
  }

  public SchemaRegistryInstance(
      String principal,
      Optional<String> topic,
      Optional<String> consumer_offsets_topic,
      Optional<String> group) {
    super(principal);
    this.topic = topic;
    this.consumer_offsets_topic = consumer_offsets_topic;
    this.group = group;
  }

  public String topicString() {
    return topic.orElse(DEFAULT_SCHEMA_TOPIC);
  }

  public String consumerOffsetsTopicString() {
    return consumer_offsets_topic.orElse(DEFAULT_CONSUMER_OFFSETS_TOPIC);
  }

  public String groupString() {
    return group.orElse(DEFAULT_SCHEMA_REGISTRY_GROUP);
  }

  public void setTopic(Optional<String> topic) {
    this.topic = topic;
  }

  public void setConsumer_offsets_topic(Optional<String> consumer_offsets_topic) {
    this.consumer_offsets_topic = consumer_offsets_topic;
  }

  public void setGroup(Optional<String> group) {
    this.group = group;
  }

  public Optional<String> getTopic() {
    return topic;
  }

  public Optional<String> getConsumer_offsets_topic() {
    return consumer_offsets_topic;
  }

  public Optional<String> getGroup() {
    return group;
  }
}
