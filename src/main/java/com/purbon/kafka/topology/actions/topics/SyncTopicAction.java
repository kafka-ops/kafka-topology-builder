package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.Subject;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SyncTopicAction extends BaseAction {

  private static final Logger LOGGER = LogManager.getLogger(SyncTopicAction.class);

  private final Topic topic;
  private final String fullTopicName;
  private final Set<String> listOfTopics;
  private final TopologyBuilderAdminClient adminClient;
  private final SchemaRegistryManager schemaRegistryManager;

  public SyncTopicAction(
      TopologyBuilderAdminClient adminClient,
      SchemaRegistryManager schemaRegistryManager,
      Topic topic,
      String fullTopicName,
      Set<String> listOfTopics) {
    this.topic = topic;
    this.fullTopicName = fullTopicName;
    this.listOfTopics = listOfTopics;
    this.adminClient = adminClient;
    this.schemaRegistryManager = schemaRegistryManager;
  }

  public String getTopic() {
    return fullTopicName;
  }

  @Override
  public void run() throws IOException {
    syncTopic(topic, fullTopicName, listOfTopics);
  }

  @Override
  public void dryRun() throws IOException {
    LOGGER.info(String.format("Checking schema compatibilities..."));
    for (TopicSchemas schema : topic.getSchemas()) {
      checkSchemaCompatibilityIfExists(schema.getKeySubject(), topic);
      checkSchemaCompatibilityIfExists(schema.getValueSubject(), topic);
    }
  }

  private void syncTopic(Topic topic, String fullTopicName, Set<String> listOfTopics)
      throws IOException {
    LOGGER.debug(String.format("Sync topic %s", fullTopicName));
    if (existTopic(fullTopicName, listOfTopics)) {
      if (topic.partitionsCount() > adminClient.getPartitionCount(fullTopicName)) {
        LOGGER.debug(String.format("Update partition count of topic %s", fullTopicName));
        adminClient.updatePartitionCount(topic, fullTopicName);
      }
      adminClient.updateTopicConfig(topic, fullTopicName);
    } else {
      LOGGER.debug(String.format("Create new topic with name %s", fullTopicName));
      adminClient.createTopic(topic, fullTopicName);
    }

    for (TopicSchemas schema : topic.getSchemas()) {
      registerSchemaIfExists(schema.getKeySubject(), topic);
      registerSchemaIfExists(schema.getValueSubject(), topic);
    }
  }

  private void checkSchemaCompatibilityIfExists(Subject subject, Topic topic) throws IOException {
    if (subject.hasSchemaFile()) {
      String schemaFile = subject.getSchemaFile();
      String subjectName = subject.buildSubjectName(topic);
      if (!schemaRegistryManager.isCompatible(subjectName, schemaFile, subject.getFormat())) {
        throw new InvalidConfigurationException(
            String.format("Incompatible schema found on topic %s and some more info...", topic));
      }
    }
  }

  private void registerSchemaIfExists(Subject subject, Topic topic) throws IOException {
    if (subject.hasSchemaFile()) {
      String keySchemaFile = subject.getSchemaFile();
      String subjectName = subject.buildSubjectName(topic);
      schemaRegistryManager.register(subjectName, keySchemaFile, subject.getFormat());
      setCompatibility(subjectName, subject.getOptionalCompatibility());
    }
  }

  private void setCompatibility(String subjectName, Optional<String> compatibilityOptional) {
    compatibilityOptional.ifPresent(
        compatibility -> schemaRegistryManager.setCompatibility(subjectName, compatibility));
  }

  private boolean existTopic(String topic, Set<String> listOfTopics) {
    return listOfTopics.contains(topic);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Topic", fullTopicName);
    String actionName = existTopic(fullTopicName, listOfTopics) ? "update" : "create";
    map.put("Action", actionName);
    return map;
  }
}
