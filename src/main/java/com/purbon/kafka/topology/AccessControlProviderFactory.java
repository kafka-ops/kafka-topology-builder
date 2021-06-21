package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.Constants.*;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.api.ccloud.CCloudCLI;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.api.mds.MDSApiClientBuilder;
import com.purbon.kafka.topology.roles.CCloudAclsProvider;
import com.purbon.kafka.topology.roles.RBACProvider;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.roles.acls.AclsBindingsBuilder;
import com.purbon.kafka.topology.roles.rbac.RBACBindingsBuilder;
import com.purbon.kafka.topology.utils.CCloudUtils;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class AccessControlProviderFactory {

  private final Configuration config;
  private final TopologyBuilderAdminClient builderAdminClient;
  private final MDSApiClientBuilder mdsApiClientBuilder;

  public AccessControlProviderFactory(
      Configuration config,
      TopologyBuilderAdminClient builderAdminClient,
      MDSApiClientBuilder mdsApiClientBuilder) {
    this.config = config;
    this.builderAdminClient = builderAdminClient;
    this.mdsApiClientBuilder = mdsApiClientBuilder;
  }

  public AccessControlProvider get() throws IOException {

    String accessControlClassName = config.getAccessControlClassName();

    try {
      Class<?> clazz = Class.forName(accessControlClassName);
      switch (accessControlClassName) {
        case ACCESS_CONTROL_DEFAULT_CLASS:
          Constructor<?> aclsProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class);
          return (SimpleAclsProvider) aclsProviderConstructor.newInstance(builderAdminClient);
        case CONFLUENT_CLOUD_CONTROL_CLASS:
          Constructor<?> ccloudProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class, Configuration.class);
          return (CCloudAclsProvider)
              ccloudProviderConstructor.newInstance(builderAdminClient, config);
        case RBAC_ACCESS_CONTROL_CLASS:
          Constructor<?> rbacProviderContructor = clazz.getConstructor(MDSApiClient.class);
          MDSApiClient apiClient = apiClientLogIn();
          if (!config.doValidate()) {
            apiClient.authenticate();
          }
          return (RBACProvider) rbacProviderContructor.newInstance(apiClient);
        default:
          throw new IOException("Unknown access control provided. " + accessControlClassName);
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public BindingsBuilderProvider builder() throws IOException {
    String accessControlClass = config.getAccessControlClassName();

    CCloudCLI cCloudApi = new CCloudCLI();
    CCloudUtils cCloudUtils = new CCloudUtils(cCloudApi, config);

    try {
      if (accessControlClass.equalsIgnoreCase(ACCESS_CONTROL_DEFAULT_CLASS)) {
        return new AclsBindingsBuilder(config, cCloudUtils);
      } else if (accessControlClass.equalsIgnoreCase(CONFLUENT_CLOUD_CONTROL_CLASS)) {
        return new AclsBindingsBuilder(config, cCloudUtils);
      } else if (accessControlClass.equalsIgnoreCase(RBAC_ACCESS_CONTROL_CLASS)) {
        MDSApiClient apiClient = apiClientLogIn();
        if (!config.doValidate()) {
          apiClient.authenticate();
        }
        return new RBACBindingsBuilder(apiClient);
      } else {
        throw new IOException(accessControlClass + " Unknown access control provided.");
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private MDSApiClient apiClientLogIn() {
    MDSApiClient apiClient = mdsApiClientBuilder.build();
    config.getMdsBasicAuth().ifPresent(apiClient::setBasicAuth);
    return apiClient;
  }
}
