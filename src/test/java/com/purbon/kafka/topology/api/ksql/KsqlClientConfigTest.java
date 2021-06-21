package com.purbon.kafka.topology.api.ksql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class KsqlClientConfigTest {

  @Test
  public void testUrlConversion() {
    KsqlClientConfig config = KsqlClientConfig.builder().setServer("https://foo.bar:9092").build();
    assertThat(config.getServer().getProtocol()).isEqualTo("https");
    assertThat(config.getServer().getPort()).isEqualTo(9092);
  }

  @Test
  public void testDefaults() {
    KsqlClientConfig config = KsqlClientConfig.builder().setServer("https://foo.bar:9092").build();
    assertThat(config.isVerifyHost()).isTrue();
    assertThat(config.useAlpn()).isFalse();
  }

  @Test
  public void testServerRequired() {
    assertThatThrownBy(() -> KsqlClientConfig.builder().build())
        .isInstanceOf(IllegalArgumentException.class);
  }
}
