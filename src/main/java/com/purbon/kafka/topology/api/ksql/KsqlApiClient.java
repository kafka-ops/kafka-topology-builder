package com.purbon.kafka.topology.api.ksql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.purbon.kafka.topology.Constants;
import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.artefact.KsqlArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlStreamArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlTableArtefact;
import com.purbon.kafka.topology.utils.JSON;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KsqlApiClient implements ArtefactClient {

  private final URL server;
  private final Client client;

  public static final String QUERY_TYPE = "query";
  public static final String STREAM_TYPE = "stream";
  public static final String TABLE_TYPE = "table";

  public KsqlApiClient(KsqlClientConfig ksqlClientConfig) {
    this.server = ksqlClientConfig.getServer();
    ClientOptions options =
        ClientOptions.create().setHost(server.getHost()).setPort(server.getPort());
    if (server.getProtocol() != null && server.getProtocol().equals(Constants.HTTPS)) {
      options.setUseTls(true);
    }
    options.setUseAlpn(ksqlClientConfig.useAlpn());
    options.setKeyStore(ksqlClientConfig.getKeyStore());
    options.setKeyStorePassword(ksqlClientConfig.getKeyStorePassword());
    options.setTrustStore(ksqlClientConfig.getTrustStore());
    options.setTrustStorePassword(ksqlClientConfig.getTrustStorePassword());
    options.setVerifyHost(ksqlClientConfig.isVerifyHost());
    if (ksqlClientConfig.useBasicAuth()) {
      options.setBasicAuthCredentials(
          ksqlClientConfig.getBasicAuth().getUser(), ksqlClientConfig.getBasicAuth().getPassword());
    }
    this.client = Client.create(options);
  }

  @Override
  public String getServer() {
    return server.toString();
  }

  @Override
  public Map<String, Object> add(String sql) throws IOException {
    try {
      var result = client.executeStatement(sql).get();
      return new QueryResponse(result).asMap();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(String id) throws IOException {
    delete(id, "STREAM");
    delete(id, "TABLE");
  }

  @Override
  public void delete(String id, String type) throws IOException {
    try {
      if (STREAM_TYPE.equalsIgnoreCase(type) || TABLE_TYPE.equalsIgnoreCase(type)) {
        String sql = String.format("DROP %s IF EXISTS %s;", type.toUpperCase(), id);
        client.executeStatement(sql).get();
      } else {
        client.terminatePushQuery(id).get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  @Override
  public List<String> list() throws IOException {
    return Stream.of(listStreams(), listTables())
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<String> listTables() throws IOException {
    List<TableInfo> infos;
    try {
      infos = client.listTables().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    return infos.stream()
        .map(tableInfo -> new KsqlTableArtefact("", server.getHost(), tableInfo.getName()))
        .map(this::artefactToString)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private String artefactToString(KsqlArtefact ksqlArtefact) {
    try {
      return JSON.asString(ksqlArtefact);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return "";
    }
  }

  public List<String> listStreams() throws IOException {
    List<StreamInfo> infos;
    try {
      infos = client.listStreams().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    return infos.stream()
        .filter(e -> !"KSQL_PROCESSING_LOG".equalsIgnoreCase(e.getName()))
        .map(queryInfo -> new KsqlStreamArtefact("", server.getHost(), queryInfo.getName()))
        .map(this::artefactToString)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends Artefact> getClusterState() throws IOException {
    List<KsqlArtefact> ksqlArtefacts = new ArrayList<>();

    listStreams().stream()
        .map(
            json -> {
              try {
                return (KsqlStreamArtefact) JSON.toObject(json, KsqlStreamArtefact.class);
              } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
              }
            })
        .filter(Objects::nonNull)
        .forEach(ksqlArtefacts::add);

    listTables().stream()
        .map(
            json -> {
              try {
                return (KsqlTableArtefact) JSON.toObject(json, KsqlTableArtefact.class);
              } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
              }
            })
        .filter(Objects::nonNull)
        .forEach(ksqlArtefacts::add);

    return ksqlArtefacts;
  }
}
