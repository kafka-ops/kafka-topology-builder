package com.purbon.kafka.topology.integration.containerutils;

import org.testcontainers.containers.BindMode;

public class SslKsqlContainer extends KsqlContainer {
  public SslKsqlContainer(AlternativeKafkaContainer kafka, String truststore, String keystore) {
    super(kafka);
    this.withEnv("KSQL_LISTENERS", "https://0.0.0.0:8088")
        .withEnv("KSQL_SSL_TRUSTSTORE_LOCATION", "/etc/ksql/secrets/truststore.jks")
        .withEnv("KSQL_SSL_TRUSTSTORE_PASSWORD", "ksqldb")
        .withEnv("KSQL_SSL_KEYSTORE_LOCATION", "/etc/ksql/secrets/keystore.jks")
        .withEnv("KSQL_SSL_KEYSTORE_PASSWORD", "ksqldb")
        .withEnv("KSQL_SSL_KEY_PASSWORD", "ksqldb")
        // Before v6.1.0: disabling TLSv1.x is required
        .withEnv("KSQL_SSL_ENABLED_PROTOCOLS", "TLSv1.3,TLSv1.2")
        .withEnv(
            "KSQL_SSL_CIPHER_SUITES",
            "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    this.withClasspathResourceMapping(
            truststore, "/etc/ksql/secrets/truststore.jks", BindMode.READ_ONLY)
        .withClasspathResourceMapping(
            keystore, "/etc/ksql/secrets/keystore.jks", BindMode.READ_ONLY);
  }

  @Override
  public String getUrl() {
    return "https://" + getHost() + ":" + getMappedPort(KSQL_PORT);
  }
}
