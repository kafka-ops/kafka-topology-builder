package com.purbon.kafka.topology.api.mds;

public class MDSRequest {
  private final String url;
  private final String jsonEntity;

  public MDSRequest(String url, String jsonEntity) {
    this.url = url;
    this.jsonEntity = jsonEntity;
  }

  public String getUrl() {
    return url;
  }

  public String getJsonEntity() {
    return jsonEntity;
  }
}
