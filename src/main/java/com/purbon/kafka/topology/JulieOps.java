package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.Constants.*;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClientBuilder;
import com.purbon.kafka.topology.api.connect.KConnectApiClient;
import com.purbon.kafka.topology.api.ksql.KsqlApiClient;
import com.purbon.kafka.topology.api.mds.MDSApiClientBuilder;
import com.purbon.kafka.topology.backend.*;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import com.purbon.kafka.topology.serviceAccounts.VoidPrincipalProvider;
import com.purbon.kafka.topology.utils.Pair;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JulieOps implements AutoCloseable {

  private static final Logger LOGGER = LogManager.getLogger(JulieOps.class);

  private TopicManager topicManager;
  private final PrincipalManager principalManager;
  private AccessControlManager accessControlManager;
  private KafkaConnectArtefactManager connectorManager;
  private KSqlArtefactManager kSqlArtefactManager;
  private final Topology topology;
  private final Configuration config;
  private final PrintStream outputStream;

  private JulieOps(
      Topology topology,
      Configuration config,
      TopicManager topicManager,
      AccessControlManager accessControlManager,
      PrincipalManager principalManager,
      KafkaConnectArtefactManager connectorManager,
      KSqlArtefactManager kSqlArtefactManager) {
    this.topology = topology;
    this.config = config;
    this.topicManager = topicManager;
    this.accessControlManager = accessControlManager;
    this.principalManager = principalManager;
    this.connectorManager = connectorManager;
    this.kSqlArtefactManager = kSqlArtefactManager;
    this.outputStream = System.out;
  }

  public static JulieOps build(String topologyFile, Map<String, String> config) throws Exception {
    return build(topologyFile, "default", config);
  }

  public static JulieOps build(String topologyFile, String plansFile, Map<String, String> config)
      throws Exception {

    verifyRequiredParameters(topologyFile, config);
    Configuration builderConfig = Configuration.build(config);
    TopologyBuilderAdminClient adminClient =
        new TopologyBuilderAdminClientBuilder(builderConfig).build();
    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(
            builderConfig, adminClient, new MDSApiClientBuilder(builderConfig));

    PrincipalProviderFactory principalProviderFactory = new PrincipalProviderFactory(builderConfig);

    return build(
        topologyFile,
        plansFile,
        builderConfig,
        adminClient,
        factory.get(),
        factory.builder(),
        principalProviderFactory.get());
  }

  public static JulieOps build(
      String topologyFileOrDir,
      Configuration config,
      TopologyBuilderAdminClient adminClient,
      AccessControlProvider accessControlProvider,
      BindingsBuilderProvider bindingsBuilderProvider)
      throws Exception {
    return build(
        topologyFileOrDir,
        "default",
        config,
        adminClient,
        accessControlProvider,
        bindingsBuilderProvider,
        new VoidPrincipalProvider());
  }

  public static JulieOps build(
      String topologyFileOrDir,
      String plansFile,
      Configuration config,
      TopologyBuilderAdminClient adminClient,
      AccessControlProvider accessControlProvider,
      BindingsBuilderProvider bindingsBuilderProvider,
      PrincipalProvider principalProvider)
      throws Exception {

    Topology topology;
    if (plansFile.equals("default")) {
      topology = TopologyObjectBuilder.build(topologyFileOrDir, config);
    } else {
      topology = TopologyObjectBuilder.build(topologyFileOrDir, plansFile, config);
    }

    TopologyValidator validator = new TopologyValidator(config);
    List<String> validationResults = validator.validate(topology);
    if (!validationResults.isEmpty()) {
      String resultsMessage = String.join("\n", validationResults);
      throw new ValidationException(resultsMessage);
    }
    config.validateWith(topology);

    AccessControlManager accessControlManager =
        new AccessControlManager(accessControlProvider, bindingsBuilderProvider, config);

    RestService restService = new RestService(config.getConfluentSchemaRegistryUrl());
    Map<String, ?> schemaRegistryConfig = config.asMap();

    List<SchemaProvider> providers =
        Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());
    SchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(
            restService,
            10,
            providers,
            schemaRegistryConfig.isEmpty() ? null : schemaRegistryConfig,
            null);
    SchemaRegistryManager schemaRegistryManager =
        new SchemaRegistryManager(schemaRegistryClient, topologyFileOrDir);

    TopicManager topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

    PrincipalManager principalManager = new PrincipalManager(principalProvider, config);

    KafkaConnectArtefactManager connectorManager =
        configureKConnectArtefactManager(config, topologyFileOrDir);

    KSqlArtefactManager kSqlArtefactManager =
        configureKSqlArtefactManager(config, topologyFileOrDir);

    return new JulieOps(
        topology,
        config,
        topicManager,
        accessControlManager,
        principalManager,
        connectorManager,
        kSqlArtefactManager);
  }

  private static KafkaConnectArtefactManager configureKConnectArtefactManager(
      Configuration config, String topologyFileOrDir) {
    Map<String, KConnectApiClient> clients =
        config.getKafkaConnectServers().entrySet().stream()
            .map(entry -> new Pair<>(entry.getKey(), new KConnectApiClient(entry.getValue())))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    if (clients.isEmpty()) {
      LOGGER.debug(
          "No KafkaConnect clients configured for JulieOps to use, please verify your config file");
    }

    return new KafkaConnectArtefactManager(clients, config, topologyFileOrDir);
  }

  private static KSqlArtefactManager configureKSqlArtefactManager(
      Configuration config, String topologyFileOrDir) {

    Map<String, KsqlApiClient> clients = new HashMap<>();
    if (config.hasKSQLServer()) {
      KsqlApiClient client = new KsqlApiClient(config.getKSQLClientConfig());
      clients.put("default", client);
    }

    if (clients.isEmpty()) {
      LOGGER.debug(
          "No KSQL clients configured for JulieOps to use, please verify your config file");
    }

    return new KSqlArtefactManager(clients, config, topologyFileOrDir);
  }

  static void verifyRequiredParameters(String topologyFile, Map<String, String> config)
      throws IOException {
    if (!Files.exists(Paths.get(topologyFile))) {
      throw new IOException("Topology file does not exist");
    }

    String configFilePath = config.get(CommandLineInterface.CLIENT_CONFIG_OPTION);
    if (!Files.exists(Paths.get(configFilePath))) {
      throw new IOException("AdminClient config file does not exist");
    }
  }

  void run(ExecutionPlan plan) throws IOException {
    LOGGER.debug(
        String.format(
            "Running topology builder with TopicManager=[%s], accessControlManager=[%s], dryRun=[%s], isQuite=[%s]",
            topicManager, accessControlManager, config.isDryRun(), config.isQuiet()));

    // Create users should always be first, so user exists when making acl link
    principalManager.applyCreate(topology, plan);

    topicManager.apply(topology, plan);
    accessControlManager.apply(topology, plan);

    connectorManager.apply(topology, plan);
    kSqlArtefactManager.apply(topology, plan);

    // Delete users should always be last,
    // avoids any unlinked acls, e.g. if acl delete or something errors then there is a link still
    // from the account, and can be re-run or manually fixed more easily
    principalManager.applyDelete(topology, plan);

    plan.run(config.isDryRun());

    if (!config.isQuiet() && !config.isDryRun()) {
      topicManager.printCurrentState(System.out);
      accessControlManager.printCurrentState(System.out);
      principalManager.printCurrentState(System.out);
      connectorManager.printCurrentState(System.out);
      kSqlArtefactManager.printCurrentState(System.out);
    }
  }

  public void run() throws IOException {
    if (config.doValidate()) {
      return;
    }
    BackendController cs = buildBackendController(config);
    ExecutionPlan plan = ExecutionPlan.init(cs, outputStream);
    run(plan);
  }

  public void close() {
    topicManager.close();
  }

  public static String getVersion() {
    InputStream resourceAsStream =
        JulieOps.class.getResourceAsStream(
            "/META-INF/maven/com.purbon.kafka/julie-ops/pom.properties");
    Properties prop = new Properties();
    try {
      prop.load(resourceAsStream);
      return prop.getProperty("version");
    } catch (IOException e) {
      e.printStackTrace();
      return "unknown";
    }
  }

  private static BackendController buildBackendController(Configuration config) throws IOException {

    String backendClass = config.getStateProcessorImplementationClassName();
    Backend backend;
    try {
      if (backendClass.equalsIgnoreCase(STATE_PROCESSOR_DEFAULT_CLASS)) {
        backend = new FileBackend();
      } else if (backendClass.equalsIgnoreCase(REDIS_STATE_PROCESSOR_CLASS)) {
        String host = config.getProperty(REDIS_HOST_CONFIG);
        int port = Integer.parseInt(config.getProperty(REDIS_PORT_CONFIG));
        backend = new RedisBackend(host, port);
      } else if (backendClass.equalsIgnoreCase(S3_STATE_PROCESSOR_CLASS)) {
        backend = new S3Backend();
      } else if (backendClass.equalsIgnoreCase(GCP_STATE_PROCESSOR_CLASS)) {
        backend = new GCPBackend();
      } else {
        throw new IOException(backendClass + " Unknown state processor provided.");
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    backend.configure(config);
    return new BackendController(backend);
  }

  void setTopicManager(TopicManager topicManager) {
    this.topicManager = topicManager;
  }

  void setAccessControlManager(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  public void setConnectorManager(KafkaConnectArtefactManager connectorManager) {
    this.connectorManager = connectorManager;
  }

  public void setKsqlArtefactManager(KSqlArtefactManager kSqlArtefactManager) {
    this.kSqlArtefactManager = kSqlArtefactManager;
  }
}
