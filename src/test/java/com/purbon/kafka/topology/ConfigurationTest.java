package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.CommandLineInterface.CLIENT_CONFIG_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.purbon.kafka.topology.api.ksql.KsqlClientConfig;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopicImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.utils.TestUtils;
import java.util.*;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationTest {

  private Map<String, String> cliOps;
  private Properties props;

  @Before
  public void before() {
    cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    props = new Properties();
  }

  @Test
  public void testWithAllRequiredFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();

    props.put(ACCESS_CONTROL_IMPLEMENTATION_CLASS, RBAC_ACCESS_CONTROL_CLASS);
    props.put(MDS_SERVER, "example.com");
    props.put(MDS_USER_CONFIG, "foo");
    props.put(MDS_PASSWORD_CONFIG, "bar");
    props.put(MDS_KAFKA_CLUSTER_ID_CONFIG, "1234");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test(expected = ConfigurationException.class)
  public void testSchemaRegistryConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    TopicSchemas schema = new TopicSchemas("foo", "bar");
    topic.setSchemas(Collections.singletonList(schema));
    project.addTopic(topic);
    topology.addProject(project);
    props.put(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "mock://");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test
  public void testSchemaRegistryValidConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    TopicSchemas schema = new TopicSchemas("foo", "bar");
    topic.setSchemas(Collections.singletonList(schema));
    project.addTopic(topic);
    topology.addProject(project);

    props.put(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "http://foo:8082");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test
  public void testSchemaRegistryValidConfigButNoSchemas() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    project.addTopic(topic);
    topology.addProject(project);

    props.put(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "http://foo:8082");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test
  public void testNoSchemaRegistry() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl("project");
    Topic topic = new TopicImpl("topic", "json");
    project.addTopic(topic);
    topology.addProject(project);

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test
  public void testPrefixValidConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    project.addTopic(topic);
    topology.addProject(project);

    props.put(TOPIC_PREFIX_FORMAT_CONFIG, "{{foo}}{{topic}}");
    props.put(PROJECT_PREFIX_FORMAT_CONFIG, "{{foo}}");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test(expected = ConfigurationException.class)
  public void testMissingPrefixValidConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    project.addTopic(topic);
    topology.addProject(project);

    props.put(TOPIC_PREFIX_FORMAT_CONFIG, "{{foo}}{{topic}}");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test(expected = ConfigurationException.class)
  public void testMissingTopicPrefixValidConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    project.addTopic(topic);
    topology.addProject(project);

    props.put(PROJECT_PREFIX_FORMAT_CONFIG, "{{foo}}{{topic}}");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test(expected = ConfigurationException.class)
  public void testIncompatiblePrefixValidConfigFields() throws ConfigurationException {
    Topology topology = new TopologyImpl();
    Project project = new ProjectImpl();
    Topic topic = new TopicImpl();
    project.addTopic(topic);
    topology.addProject(project);

    props.put(PROJECT_PREFIX_FORMAT_CONFIG, "{{banana}}");

    Configuration config = new Configuration(cliOps, props);
    config.validateWith(topology);
  }

  @Test
  public void testKafkaInternalTopicDefaultPrefix() {
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");

    cliOps.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    Configuration config = Configuration.build(cliOps);
    assertThat(config.getKafkaInternalTopicPrefixes()).isEqualTo(Collections.singletonList("_"));
  }

  @Test
  public void testKafkaInternalTopicExtendedPrefix() {
    String clientConfigFile =
        TestUtils.getResourceFilename("/config-internals-extended.properties");

    cliOps.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    Configuration config = Configuration.build(cliOps);
    assertThat(config.getKafkaInternalTopicPrefixes())
        .isEqualTo(Arrays.asList("_", "topicA", "topicB"));
  }

  @Test
  public void testKsqlServerWithHttps() {
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");
    cliOps.put(CLIENT_CONFIG_OPTION, clientConfigFile);
    props.put(PLATFORM_SERVER_KSQL_URL, "https://example.com:8083");
    Configuration config = new Configuration(cliOps, props);
    KsqlClientConfig ksqlClientConfig = config.getKSQLClientConfig();

    assertThat(ksqlClientConfig.getServer().getProtocol()).isEqualTo("https");
    assertThat(ksqlClientConfig.getServer().getHost()).isEqualTo("example.com");
    assertThat(ksqlClientConfig.getServer().getPort()).isEqualTo(8083);
  }

  @Test
  public void testKsqlServerWithoutScheme() {
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");
    cliOps.put(CLIENT_CONFIG_OPTION, clientConfigFile);
    props.put(PLATFORM_SERVER_KSQL_URL, "example.com:8083");
    Configuration config = new Configuration(cliOps, props);

    assertThatThrownBy(config::getKSQLClientConfig)
        .hasMessageContaining("example.com:8083")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
