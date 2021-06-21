package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.api.mds.MDSApiClientBuilder;
import com.purbon.kafka.topology.roles.RBACProvider;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.utils.BasicAuth;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AccessControlProviderFactoryTest {

  @Mock TopologyBuilderAdminClient adminClient;

  @Mock MDSApiClientBuilder mdsApiClientBuilder;

  @Mock MDSApiClient mdsApiClient;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  Map<String, String> cliOps;
  Properties props;

  @Before
  public void before() {
    cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    props = new Properties();
  }

  @Test
  public void testRBACConfig() throws IOException {

    props.put(ACCESS_CONTROL_IMPLEMENTATION_CLASS, "com.purbon.kafka.topology.roles.RBACProvider");
    props.put(MDS_SERVER, "http://localhost:8090");
    props.put(MDS_USER_CONFIG, "alice");
    props.put(MDS_PASSWORD_CONFIG, "alice-secret");
    props.put(MDS_KAFKA_CLUSTER_ID_CONFIG, "UtBZ3rTSRtypmmkAL1HbHw");

    Configuration config = new Configuration(cliOps, props);

    when(mdsApiClientBuilder.build()).thenReturn(mdsApiClient);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);

    AccessControlProvider provider = factory.get();

    verify(mdsApiClient, times(1)).setBasicAuth(new BasicAuth("alice", "alice-secret"));
    verify(mdsApiClient, times(1)).authenticate();

    assertThat(provider, instanceOf(RBACProvider.class));
  }

  @Test
  public void testACLsConfig() throws IOException {

    Configuration config = new Configuration(cliOps, props);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);

    assertThat(factory.get(), instanceOf(SimpleAclsProvider.class));
  }

  @Test(expected = IOException.class)
  public void testWrongProviderConfig() throws IOException {

    props.put(
        ACCESS_CONTROL_IMPLEMENTATION_CLASS, "com.purbon.kafka.topology.roles.MyCustomProvider");

    Configuration config = new Configuration(cliOps, props);

    when(mdsApiClientBuilder.build()).thenReturn(mdsApiClient);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);
    factory.get();
  }
}
