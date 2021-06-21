package com.purbon.kafka.topology.model.artefact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@TypeArtefact(name = "STREAM")
public class KsqlStreamArtefact extends KsqlArtefact {

  @JsonCreator
  public KsqlStreamArtefact(
      @JsonProperty("path") String path,
      @JsonProperty("server") String label,
      @JsonProperty("name") String name) {
    super(path, label, name);
  }

  @Override
  public String toString() {
    String serverLabel = getServerLabel() == null ? "" : String.format(" (%s)", getServerLabel());
    return "STREAM " + getName() + serverLabel;
  }
}
