package com.purbon.kafka.topology.utils;

import java.util.Base64;
import java.util.Objects;

public class BasicAuth {
  private final String user;
  private final String password;

  public BasicAuth(String user, String password) {
    this.user = Objects.requireNonNull(user);
    this.password = Objects.requireNonNull(password);
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  /**
   * Creates the value as it can be used for an Authorization header in an HTTP request
   *
   * @return Basic + encoded credentials
   */
  public String toHttpAuthToken() {
    String userAndPassword = user + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(userAndPassword.getBytes());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BasicAuth basicAuth = (BasicAuth) o;
    return user.equals(basicAuth.user) && password.equals(basicAuth.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, password);
  }
}
