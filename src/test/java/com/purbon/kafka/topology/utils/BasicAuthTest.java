package com.purbon.kafka.topology.utils;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Test;

public class BasicAuthTest {

  @Test
  public void testToHttpAuthToken() {
    BasicAuth auth = new BasicAuth("user", "pass");
    assertThat(auth.toHttpAuthToken()).isEqualTo("Basic dXNlcjpwYXNz");
  }

  @Test
  public void testValuesRequired() {
    assertThatThrownBy(() -> new BasicAuth(null, "pass")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BasicAuth("user", null)).isInstanceOf(NullPointerException.class);
  }
}
