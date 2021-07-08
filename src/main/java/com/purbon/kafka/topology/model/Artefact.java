package com.purbon.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Artefact {

  private String path;

  private String serverLabel;
  private String name;

  public Artefact(String path, String serverLabel, String name) {
    this.path = path;
    this.serverLabel = serverLabel;
    this.name = name;
  }

  public String getPath() {
    return path;
  }

  public String getServerLabel() {
    return serverLabel;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Artefact)) return false;
    Artefact artefact = (Artefact) o;
    if (serverLabel != null) {
      return getServerLabel().equalsIgnoreCase(artefact.getServerLabel())
          && Objects.equals(name.toLowerCase(), artefact.getName().toLowerCase());
    } else {
      return Objects.equals(name.toLowerCase(), artefact.getName().toLowerCase());
    }
  }

  @Override
  public int hashCode() {
    if (serverLabel != null) {
      return Objects.hash(getServerLabel().toLowerCase(), getName().toLowerCase());
    } else {
      return Objects.hash(getName().toLowerCase());
    }
  }
}
