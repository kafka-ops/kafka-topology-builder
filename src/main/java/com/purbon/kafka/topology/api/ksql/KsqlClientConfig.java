package com.purbon.kafka.topology.api.ksql;

import com.purbon.kafka.topology.utils.BasicAuth;
import java.net.MalformedURLException;
import java.net.URL;

public class KsqlClientConfig {

  private final URL server;
  private final BasicAuth basicAuth;
  private final boolean useAlpn;
  private final String keyStore;
  private final String keyStorePassword;
  private final String trustStore;
  private final String trustStorePassword;
  private final boolean verifyHost;

  private KsqlClientConfig(Builder builder) {
    try {
      this.server = new URL(builder.server);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(String.format("%s is not a valid URL", builder.server), e);
    }
    this.basicAuth = builder.basicAuth;
    this.useAlpn = builder.useAlpn;
    this.keyStore = builder.keyStore;
    this.keyStorePassword = builder.keyStorePassword;
    this.trustStore = builder.trustStore;
    this.trustStorePassword = builder.trustStorePassword;
    this.verifyHost = builder.verifyHost;
  }

  public static Builder builder() {
    return new Builder();
  }

  public URL getServer() {
    return this.server;
  }

  public boolean useAlpn() {
    return useAlpn;
  }

  public String getKeyStore() {
    return keyStore;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStore() {
    return trustStore;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public boolean isVerifyHost() {
    return verifyHost;
  }

  public boolean useBasicAuth() {
    return basicAuth != null;
  }

  public BasicAuth getBasicAuth() {
    return basicAuth;
  }

  public static class Builder {
    private String server;
    private BasicAuth basicAuth;
    private boolean useAlpn;
    private String keyStore;
    private String keyStorePassword;
    private String trustStore;
    private String trustStorePassword;
    private boolean verifyHost = true;

    public Builder setServer(String server) {
      this.server = server;
      return this;
    }

    public Builder setBasicAuth(BasicAuth basicAuth) {
      this.basicAuth = basicAuth;
      return this;
    }

    public Builder setUseAlpn(boolean useAlpn) {
      this.useAlpn = useAlpn;
      return this;
    }

    public Builder setKeyStore(String keyStore) {
      this.keyStore = keyStore;
      return this;
    }

    public Builder setKeyStorePassword(String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    public Builder setTrustStore(String trustStore) {
      this.trustStore = trustStore;
      return this;
    }

    public Builder setTrustStorePassword(String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
    }

    public Builder setVerifyHost(boolean verifyHost) {
      this.verifyHost = verifyHost;
      return this;
    }

    public KsqlClientConfig build() {
      return new KsqlClientConfig(this);
    }
  }
}
