package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.BackendController;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.ExecutionPlan;
import com.purbon.kafka.topology.KSqlArtefactManager;
import com.purbon.kafka.topology.api.ksql.KsqlApiClient;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.KsqlContainer;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KsqlManagerIT {

  static SaslPlaintextKafkaContainer container;
  static KsqlContainer ksqlContainer;
  private TopologySerdes parser;
  private KsqlApiClient client;
  private ExecutionPlan plan;

  @After
  public void after() {
    ksqlContainer.stop();
    container.stop();
  }

  @Before
  public void configure() throws InterruptedException, IOException {
    container = ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));
    container.start();
    ksqlContainer = new KsqlContainer(container);
    ksqlContainer.start();
    Thread.sleep(3000);

    Files.deleteIfExists(Paths.get(".cluster-state"));

    client = new KsqlApiClient(ksqlContainer.getHost(), ksqlContainer.getPort());
    parser = new TopologySerdes();

    this.plan = ExecutionPlan.init(new BackendController(), System.out);
  }

  @Test
  public void testCreateAndUpdatePathWithLocalClusterState() throws IOException {
    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_KSQL_ARTEFACTS, "true");

    File file = TestUtils.getResourceFile("/descriptor-ksql.yaml");

    testCreateAndUpdatePath(props, file);
  }

  @Test
  public void testCreateAndUpdatePathWithRemoveClusterState() throws IOException {
    Properties props = new Properties();
    props.put(TOPOLOGY_STATE_FROM_CLUSTER, "true");
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_KSQL_ARTEFACTS, "true");
    props.put(PLATFORM_SERVER_KSQL, "http://" + client.getServer());

    File file = TestUtils.getResourceFile("/descriptor-ksql.yaml");

    testCreateAndUpdatePath(props, file);
  }

  public void testCreateAndUpdatePath(Properties props, File file) throws IOException {

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    Configuration config = new Configuration(cliOps, props);

    Topology topology = parser.deserialise(file);

    KSqlArtefactManager kam = new KSqlArtefactManager(client, config, file.getAbsolutePath());

    kam.apply(topology, plan);
    plan.run();

    List<String> streams = client.listStreams();
    assertThat(streams).hasSize(1);
    assertThat(streams).contains("{\"path\":\"\",\"name\":\"RIDERLOCATIONS\"}");

    topology.getProjects().get(0).getKsqlArtefacts().getStreams().remove(0);

    assertThat(topology.getProjects().get(0).getKsqlArtefacts().getStreams()).hasSize(0);

    ExecutionPlan newPlan = ExecutionPlan.init(new BackendController(), System.out);

    kam.apply(topology, newPlan);
    newPlan.run();

    streams = client.listStreams();
    assertThat(streams).hasSize(0);
  }
}
