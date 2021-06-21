package com.purbon.kafka.topology.integration.containerutils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class KsqlContainer extends GenericContainer<KsqlContainer> {

  private static final DockerImageName DEFAULT_IMAGE =
      DockerImageName.parse("confluentinc/ksqldb-server").withTag("0.17.0");

  public static final int KSQL_PORT = 8088;

  public KsqlContainer(AlternativeKafkaContainer kafka) {
    this(DEFAULT_IMAGE, kafka);
  }

  public KsqlContainer(final DockerImageName dockerImageName, AlternativeKafkaContainer kafka) {
    super(dockerImageName);
    String kafkaHost = kafka.getNetworkAliases().get(1);
    withExposedPorts(KSQL_PORT);
    waitingFor(Wait.forLogMessage(".+ INFO Server up and running .+", 1));
    // withEnv("KSQL_KSQL_SERVICE_ID", "confluent_ksql_streams_01");
    withEnv("KSQL_SECURITY_PROTOCOL", "SASL_PLAINTEXT");
    withEnv("KSQL_BOOTSTRAP_SERVERS", "SASL_PLAINTEXT://" + kafkaHost + ":" + 9091);
    withEnv("KSQL_SASL_JAAS_CONFIG", saslConfig());
    withEnv("KSQL_SASL_MECHANISM", "PLAIN");
    withEnv("KSQL_LISTENERS", "http://0.0.0.0:8088");
    withEnv("KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE", "true");
    withEnv("KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE", "true");
    withNetwork(kafka.getNetwork());
  }

  private String saslConfig() {
    StringBuilder sb = new StringBuilder();
    sb.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
    sb.append("kafka");
    sb.append("\" password=\"");
    sb.append("kafka");
    sb.append("\";");
    return sb.toString();
  }

  public String getUrl() {
    return "http://" + getHost() + ":" + getMappedPort(KSQL_PORT);
  }

  public Integer getPort() {
    return getMappedPort(KSQL_PORT);
  }
}
