package com.purbon.kafka.topology.clients;

import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;

import com.purbon.kafka.topology.api.mds.Response;
import com.purbon.kafka.topology.utils.BasicAuth;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JulieHttpClient {

  private static final Logger LOGGER = LogManager.getLogger(JulieHttpClient.class);

  private final long DEFAULT_TIMEOUT_MS = 60000;

  private final HttpClient httpClient = HttpClient.newBuilder().build();
  protected final String server;
  private String token;

  public JulieHttpClient(String server) {
    this.server = server;
  }

  private HttpRequest.Builder setupARequest(String url, long timeoutMs) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(server + url))
            .timeout(Duration.ofMillis(timeoutMs))
            .header("accept", " application/json")
            .header("Content-Type", "application/json");
    if (!token.isBlank()) {
      builder = builder.header("Authorization", token);
    }
    return builder;
  }

  public void setBasicAuth(BasicAuth basicAuth) {
    this.token = basicAuth.toHttpAuthToken();
  }

  protected Response doGet(String url) throws IOException {
    HttpRequest request = getRequest(url, DEFAULT_TIMEOUT_MS);
    return doGet(request);
  }

  private HttpRequest getRequest(String url, long timeoutMs) {
    return setupARequest(url, timeoutMs).GET().build();
  }

  private Response doGet(HttpRequest request) throws IOException {
    LOGGER.debug("method: " + request.method() + " request.uri: " + request.uri());
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      LOGGER.debug("method: " + request.method() + " response: " + response);
      return new Response(response);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  protected String doPost(String url, String body) throws IOException {
    LOGGER.debug("doPost: " + url + " body: " + body);
    HttpRequest request = postRequest(url, body, DEFAULT_TIMEOUT_MS);
    return doRequest(request);
  }

  private HttpRequest postRequest(String url, String body, long timeoutMs) {
    return setupARequest(url, timeoutMs).POST(ofString(body)).build();
  }

  protected void doPut(String url) throws IOException {
    LOGGER.debug("doPut: " + url);
    HttpRequest request = putRequest(url, DEFAULT_TIMEOUT_MS);
    doRequest(request);
  }

  private HttpRequest putRequest(String url, long timeoutMs) {
    return setupARequest(url, timeoutMs).PUT(noBody()).build();
  }

  protected void doDelete(String url, String body) throws IOException {
    LOGGER.debug("doDelete: " + url + " body: " + body);
    HttpRequest request = deleteRequest(url, body, DEFAULT_TIMEOUT_MS);
    doRequest(request);
  }

  private HttpRequest deleteRequest(String url, String body, long timeoutMs) {
    HttpRequest.Builder builder = setupARequest(url, timeoutMs);
    BodyPublisher bodyPublisher = !body.isEmpty() ? ofString(body) : noBody();
    builder = builder.method("DELETE", bodyPublisher);
    return builder.build();
  }

  private String doRequest(HttpRequest request) throws IOException {
    LOGGER.debug("method: " + request.method() + " request.uri: " + request.uri());
    String result = "";
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      LOGGER.debug("method: " + request.method() + " response: " + response);
      int statusCode = response.statusCode();
      if (statusCode < 200 || statusCode > 299) {
        String body = response.body() != null ? response.body() : "";
        throw new IOException(
            "Something happened with the connection, response status code: "
                + statusCode
                + " body: "
                + body);
      }

      if (response.body() != null) {
        result = response.body();
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return result;
  }
}
