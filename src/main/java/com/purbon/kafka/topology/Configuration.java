package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;

import com.purbon.kafka.topology.api.ksql.KsqlClientConfig;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.serdes.TopologySerdes.FileType;
import com.purbon.kafka.topology.utils.BasicAuth;
import com.purbon.kafka.topology.utils.Pair;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class Configuration {

  private final Map<String, String> cliParams;
  private final Config config;

  public Configuration() {
    this(new HashMap<>(), ConfigFactory.load());
  }

  public static Configuration build(Map<String, String> cliParams) {
    return build(cliParams, cliParams.get(CLIENT_CONFIG_OPTION));
  }

  public static Configuration build(Map<String, String> cliParams, String configFile) {
    if (!configFile.isEmpty()) {
      System.setProperty("config.file", configFile);
    }
    ConfigFactory.invalidateCaches();
    Config config = ConfigFactory.load();

    String overridingConfigFile = cliParams.get(OVERRIDING_CLIENT_CONFIG_OPTION);
    if (overridingConfigFile != null) {
      Config overridingConfig = ConfigFactory.parseFile(new File(overridingConfigFile));
      config = overridingConfig.withFallback(config);
    }
    return new Configuration(cliParams, config);
  }

  public Configuration(Map<String, String> cliParams, Properties props) {
    this(cliParams, (Map) props);
  }

  public Configuration(Map<String, String> cliParams, Map<String, Object> props) {
    this(cliParams, ConfigFactory.parseMap(props).withFallback(ConfigFactory.load()));
  }

  public Configuration(Map<String, String> cliParams, Config config) {
    this.cliParams = cliParams;
    this.config = config;
  }

  public Map<String, ?> asMap() {
    return asMap("");
  }

  public Map<String, ?> asMap(String filter) {
    Map<String, Object> map = new HashMap<>();
    config.entrySet().stream()
        .filter(entry -> filter.isEmpty() || entry.getKey().startsWith(filter))
        .forEach(entry -> map.put(entry.getKey(), entry.getValue().unwrapped()));
    return map;
  }

  public Properties asProperties() {
    Properties props = new Properties();
    config.entrySet().forEach(entry -> props.put(entry.getKey(), entry.getValue().unwrapped()));
    if (cliParams.get(CommandLineInterface.BROKERS_OPTION) != null) {
      props.put(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          cliParams.get(CommandLineInterface.BROKERS_OPTION));
    }
    props.put(AdminClientConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    return props;
  }

  public void validateWith(Topology topology) throws ConfigurationException {
    validateGeneralConfiguration(topology);
    boolean isRBAC = this.getAccessControlClassName().equalsIgnoreCase(RBAC_ACCESS_CONTROL_CLASS);
    if (isRBAC) {
      validateRBACConfiguration(topology);
    }
  }

  public void validateRBACConfiguration(Topology topology) throws ConfigurationException {
    raiseIfNull(MDS_SERVER, MDS_USER_CONFIG, MDS_PASSWORD_CONFIG);
    raiseIfNull(MDS_KAFKA_CLUSTER_ID_CONFIG);

    final boolean hasSchemaRegistry = !topology.getPlatform().getSchemaRegistry().isEmpty();
    final boolean hasKafkaConnect =
        !topology.getProjects().stream().allMatch(project -> project.getConnectors().isEmpty());

    if (hasSchemaRegistry) {
      raiseIfNull(MDS_SR_CLUSTER_ID_CONFIG);
    } else if (hasKafkaConnect && config.getString(MDS_KC_CLUSTER_ID_CONFIG) == null) {
      raiseIfNull(MDS_KC_CLUSTER_ID_CONFIG);
    }
  }

  private void validateGeneralConfiguration(Topology topology) throws ConfigurationException {
    if (hasSchemas(topology)) {
      raiseIfDefault(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "mock://");
    }

    validateBrokersConfig();

    boolean topicPrefixDefinedButNotProjectPrefix =
        !getTopicPrefixFormat().equals("default") && getProjectPrefixFormat().equals("default");

    boolean projectPrefixDefinedButNotTopicPrefix =
        getTopicPrefixFormat().equals("default") && !getProjectPrefixFormat().equals("default");

    if (topicPrefixDefinedButNotProjectPrefix || projectPrefixDefinedButNotTopicPrefix) {
      throw new ConfigurationException(
          TOPIC_PREFIX_FORMAT_CONFIG
              + " and "
              + PROJECT_PREFIX_FORMAT_CONFIG
              + " need to be defined together.");
    }

    if (!getTopicPrefixFormat().startsWith(getProjectPrefixFormat())) {
      throw new ConfigurationException(
          TOPIC_PREFIX_FORMAT_CONFIG + "should start by" + PROJECT_PREFIX_FORMAT_CONFIG);
    }
  }

  private void validateBrokersConfig() throws ConfigurationException {
    boolean existServersAsConfig;
    try {
      config.getString(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
      existServersAsConfig = true;
    } catch (Exception ex) {
      existServersAsConfig = false;
    }

    if (cliParams.get(CommandLineInterface.BROKERS_OPTION) == null && !existServersAsConfig) {
      String msg =
          String.format(
              "Either the CLI option %s or the configuration %s should be specified",
              CommandLineInterface.BROKERS_OPTION, AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
      throw new ConfigurationException(msg);
    }
  }

  private static boolean hasSchemas(Topology topology) {
    return topology.getProjects().stream()
        .flatMap((Function<Project, Stream<Topic>>) project -> project.getTopics().stream())
        .anyMatch(topic -> !topic.getSchemas().isEmpty());
  }

  private void raiseIfDefault(String key, String _default) throws ConfigurationException {
    if (config.getString(key).equals(_default)) {
      throw new ConfigurationException(
          "Configuration key " + key + " should not have the default value " + _default);
    }
  }

  private void raiseIfNull(String... keys) throws ConfigurationException {
    try {
      for (String key : keys) {
        config.getString(key);
      }
    } catch (Exception ex) {
      throw new ConfigurationException(ex.getMessage());
    }
  }

  public String getProperty(String key) {
    return config.getString(key);
  }

  public List<String> getKafkaInternalTopicPrefixes() {
    return config.getStringList(KAFKA_INTERNAL_TOPIC_PREFIXES).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  public List<String> getServiceAccountManagedPrefixes() {
    return config.getStringList(SERVICE_ACCOUNT_MANAGED_PREFIXES).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  public List<String> getTopicManagedPrefixes() {
    return config.getStringList(TOPIC_MANAGED_PREFIXES).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  public List<String> getGroupManagedPrefixes() {
    return config.getStringList(GROUP_MANAGED_PREFIXES).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  public String getConfluentSchemaRegistryUrl() {
    return config.getString(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG);
  }

  public String getConfluentMonitoringTopic() {
    return config.getString(CONFLUENT_MONITORING_TOPIC_CONFIG);
  }

  public String getConfluentCommandTopic() {
    return config.getString(CONFLUENT_COMMAND_TOPIC_CONFIG);
  }

  public String getConfluentMetricsTopic() {
    return config.getString(CONFLUENT_METRICS_TOPIC_CONFIG);
  }

  public String getAccessControlClassName() {
    return config.getString(ACCESS_CONTROL_IMPLEMENTATION_CLASS);
  }

  public String getStateProcessorImplementationClassName() {
    return config.getString(STATE_PROCESSOR_IMPLEMENTATION_CLASS);
  }

  public String getTopicPrefixFormat() {
    return config.getString(TOPIC_PREFIX_FORMAT_CONFIG);
  }

  public String getProjectPrefixFormat() {
    return config.getString(PROJECT_PREFIX_FORMAT_CONFIG);
  }

  public String getTopicPrefixSeparator() {
    return config.getString(TOPIC_PREFIX_SEPARATOR_CONFIG);
  }

  public Boolean shouldOptimizeAcls() {
    return config.getBoolean(OPTIMIZED_ACLS_CONFIG);
  }

  public String getConfluentCloudEnv() {
    return config.getString(CCLOUD_ENV_CONFIG);
  }

  public boolean enabledExperimental() {
    return config.getBoolean(TOPOLOGY_EXPERIMENTAL_ENABLED_CONFIG);
  }

  public boolean useConfuentCloud() {
    return config.hasPath(CCLOUD_ENV_CONFIG);
  }

  public Optional<String> getInternalPrincipalOptional() {
    String internalPrincipal = null;
    if (hasProperty(JULIE_INTERNAL_PRINCIPAL)) {
      internalPrincipal = config.getString(JULIE_INTERNAL_PRINCIPAL);
    } else if (hasProperty(TOPOLOGY_BUILDER_INTERNAL_PRINCIPAL)) {
      internalPrincipal = config.getString(TOPOLOGY_BUILDER_INTERNAL_PRINCIPAL);
    }
    return Optional.ofNullable(internalPrincipal);
  }

  public boolean hasProperty(String property) {
    return config.hasPath(property);
  }

  public List<String> getTopologyValidations() {
    List<String> classes = config.getStringList(TOPOLOGY_VALIDATIONS_CONFIG);
    return classes.stream().map(String::trim).collect(Collectors.toList());
  }

  public boolean enabledConnectorTopicCreateAcl() {
    return config.getBoolean(CONNECTOR_ALLOW_TOPIC_CREATE);
  }

  public boolean isQuiet() {
    return Boolean.parseBoolean(cliParams.getOrDefault(CommandLineInterface.QUIET_OPTION, "false"));
  }

  public boolean doValidate() {
    return Boolean.parseBoolean(
        cliParams.getOrDefault(CommandLineInterface.VALIDATE_OPTION, "false"));
  }

  public boolean isDryRun() {
    return Boolean.parseBoolean(cliParams.getOrDefault(DRY_RUN_OPTION, "false"));
  }

  public FileType getTopologyFileType() {
    return config.getEnum(FileType.class, TOPOLOGY_FILE_TYPE);
  }

  public boolean isAllowDeleteTopics() {
    return config.getBoolean(ALLOW_DELETE_TOPICS);
  }

  public boolean isAllowDeleteBindings() {
    return config.getBoolean(ALLOW_DELETE_BINDINGS);
  }

  public boolean isAllowDeletePrincipals() {
    return config.getBoolean(ALLOW_DELETE_PRINCIPALS);
  }

  public boolean isAllowDeleteConnectArtefacts() {
    return config.getBoolean(ALLOW_DELETE_CONNECT_ARTEFACTS);
  }

  public boolean isAllowDeleteKsqlArtefacts() {
    return config.getBoolean(ALLOW_DELETE_KSQL_ARTEFACTS);
  }

  public boolean enabledPrincipalTranslation() {
    return config.getBoolean(TOPOLOGY_PRINCIPAL_TRANSLATION_ENABLED_CONFIG);
  }

  public boolean fetchStateFromTheCluster() {
    return config.getBoolean(TOPOLOGY_STATE_FROM_CLUSTER);
  }

  public boolean fetchTopicStateFromTheCluster() {
    return fetchStateFromTheCluster() || config.getBoolean(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER);
  }

  public String getS3Bucket() {
    return config.getString(JULIE_S3_BUCKET);
  }

  public String getS3Region() {
    return config.getString(JULIE_S3_REGION);
  }

  public String getGCPProjectId() {
    return config.getString(JULIE_GCP_PROJECT_ID);
  }

  public String getGCPBucket() {
    return config.getString(JULIE_GCP_BUCKET);
  }

  public String getMdsServer() {
    return config.getString(MDS_SERVER);
  }

  public String getKafkaClusterId() {
    return config.getString(MDS_KAFKA_CLUSTER_ID_CONFIG);
  }

  public String getSchemaRegistryClusterId() {
    return config.getString(MDS_SR_CLUSTER_ID_CONFIG);
  }

  public String getKafkaConnectClusterId() {
    return config.getString(MDS_KC_CLUSTER_ID_CONFIG);
  }

  public Map<String, String> getKafkaConnectServers() {
    List<String> servers = config.getStringList(PLATFORM_SERVERS_CONNECT);
    return servers.stream()
        .map(server -> server.split(":"))
        .map(
            strings -> {
              String key = strings[0].strip();
              String value = String.join(":", Arrays.copyOfRange(strings, 1, strings.length));
              return new Pair<>(key, value);
            })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public KsqlClientConfig getKSQLClientConfig() {
    KsqlClientConfig.Builder ksqlConf =
        new KsqlClientConfig.Builder()
            .setServer(getProperty(PLATFORM_SERVER_KSQL_URL))
            .setTrustStore(getPropertyOrNull(PLATFORM_SERVER_KSQL_TRUSTSTORE))
            .setTrustStorePassword(getPropertyOrNull(PLATFORM_SERVER_KSQL_TRUSTSTORE_PW))
            .setKeyStore(getPropertyOrNull(PLATFORM_SERVER_KSQL_KEYSTORE))
            .setKeyStorePassword(getPropertyOrNull(PLATFORM_SERVER_KSQL_KEYSTORE_PW));
    if (hasProperty(PLATFORM_SERVER_KSQL_ALPN)) {
      ksqlConf.setUseAlpn(config.getBoolean(PLATFORM_SERVER_KSQL_ALPN));
    }
    if (hasProperty(PLATFORM_SERVER_KSQL_BASIC_AUTH_PASSWORD)
        && hasProperty(PLATFORM_SERVER_KSQL_BASIC_AUTH_USER)) {
      ksqlConf.setBasicAuth(
          new BasicAuth(
              getProperty(PLATFORM_SERVER_KSQL_BASIC_AUTH_USER),
              getProperty(PLATFORM_SERVER_KSQL_BASIC_AUTH_PASSWORD)));
    }

    if (hasProperty(PLATFORM_SERVER_KSQL_VERIFY_HOST)) {
      ksqlConf.setVerifyHost(config.getBoolean(PLATFORM_SERVER_KSQL_VERIFY_HOST));
    }
    return ksqlConf.build();
  }

  public boolean hasKSQLServer() {
    return config.hasPath(PLATFORM_SERVER_KSQL_URL);
  }

  private String getPropertyOrNull(String key) {
    try {
      return config.getString(key);
    } catch (ConfigException.Missing e) {
      return null;
    }
  }

  public Optional<BasicAuth> getMdsBasicAuth() {
    BasicAuth auth = null;
    if (hasProperty(MDS_USER_CONFIG) && hasProperty(MDS_PASSWORD_CONFIG)) {
      auth = new BasicAuth(getProperty(MDS_USER_CONFIG), getProperty(MDS_PASSWORD_CONFIG));
    }
    return Optional.ofNullable(auth);
  }
}
