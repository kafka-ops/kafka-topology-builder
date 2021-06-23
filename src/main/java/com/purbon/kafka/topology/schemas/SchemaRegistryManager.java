package com.purbon.kafka.topology.schemas;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaRegistryManager {

  private static final Logger LOGGER = LogManager.getLogger(SchemaRegistryManager.class);

  static class SchemaRegistryManagerException extends RuntimeException {
    public SchemaRegistryManagerException(String message) {
      super(message);
    }

    public SchemaRegistryManagerException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final SchemaRegistryClient schemaRegistryClient;
  private final String rootPath;

  public SchemaRegistryManager(
      SchemaRegistryClient schemaRegistryClient, String topologyFileOrDir) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.rootPath =
        Files.isDirectory(Paths.get(topologyFileOrDir))
            ? topologyFileOrDir
            : new File(topologyFileOrDir).getParent();
  }

  public int register(String subjectName, String schemaFile, String format) {
    try {
      return register(subjectName, schemaFilePath(schemaFile), format);
    } catch (Exception e) {
      throw new SchemaRegistryManagerException("Failed to parse the schema file " + schemaFile, e);
    }
  }

  public int register(String subjectName, Path schemaFilePath, String format) {
    LOGGER.debug(
        String.format("Registering subject %s with source %s", subjectName, schemaFilePath));
    try {
      final String schema = new String(Files.readAllBytes(schemaFilePath), StandardCharsets.UTF_8);
      return save(subjectName, format, schema);
    } catch (Exception e) {
      throw new SchemaRegistryManagerException(
          "Failed to parse the schema file " + schemaFilePath, e);
    }
  }

  public String setCompatibility(String subject, String compatibility) {
    try {
      return schemaRegistryClient.updateCompatibility(subject, compatibility);
    } catch (Exception e) {
      final String msg =
          String.format(
              "Failed to register the schema compatibility mode '%s' for subject '%s'",
              compatibility, subject);
      throw new SchemaRegistryManagerException(msg, e);
    }
  }

  private Path schemaFilePath(String schemaFile) {
    Path mayBeAbsolutePath = Paths.get(schemaFile);
    Path path =
        mayBeAbsolutePath.isAbsolute() ? mayBeAbsolutePath : Paths.get(rootPath, schemaFile);
    LOGGER.debug(String.format("SchemaFile %s loaded from %s", schemaFile, path));
    return path;
  }

  protected int save(String subjectName, String schemaType, String schemaString) {
    final Optional<ParsedSchema> maybeSchema =
        schemaRegistryClient.parseSchema(schemaType, schemaString, Collections.emptyList());

    final ParsedSchema parsedSchema =
        maybeSchema.orElseThrow(
            () -> {
              final String msg =
                  String.format(
                      "Failed to parse the schema for subject '%s' of type '%s'",
                      subjectName, schemaType);
              return new SchemaRegistryManagerException(msg);
            });

    try {
      return schemaRegistryClient.register(subjectName, parsedSchema);
    } catch (Exception e) {
      final String msg =
          String.format(
              "Failed to register the schema for subject '%s' of type '%s'",
              subjectName, schemaType);
      throw new SchemaRegistryManagerException(msg, e);
    }
  }

  public boolean isCompatible(String subjectName, String schemaFile, String format) {
    try {
      return isCompatible(subjectName, schemaFilePath(schemaFile), format);
    } catch (Exception e) {
      throw new SchemaRegistryManagerException("Failed to parse the schema file " + schemaFile, e);
    }
  }

  public boolean isCompatible(String subjectName, Path schemaFilePath, String format) {
    LOGGER.debug(
        String.format("Registering subject %s with source %s", subjectName, schemaFilePath));
    try {
      final String schema = new String(Files.readAllBytes(schemaFilePath), StandardCharsets.UTF_8);
      return testCompatibility(subjectName, format, schema);
    } catch (Exception e) {
      throw new SchemaRegistryManagerException(
          "Failed to parse the schema file " + schemaFilePath, e);
    }
  }

  protected boolean testCompatibility(String subjectName, String schemaType, String schemaString) {
    final Optional<ParsedSchema> maybeSchema =
        schemaRegistryClient.parseSchema(schemaType, schemaString, Collections.emptyList());

    final ParsedSchema parsedSchema =
        maybeSchema.orElseThrow(
            () -> {
              final String msg =
                  String.format(
                      "Failed to parse the schema for subject '%s' of type '%s'",
                      subjectName, schemaType);
              return new SchemaRegistryManagerException(msg);
            });

    try {
      return schemaRegistryClient.testCompatibility(subjectName, parsedSchema);
    } catch (Exception e) {
      final String msg =
          String.format(
              "Failed to test compatibility of schema for subject '%s' of type '%s'",
              subjectName, schemaType);
      throw new SchemaRegistryManagerException(msg, e);
    }
  }
}
