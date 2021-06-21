package com.purbon.kafka.topology;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.artefact.KsqlArtefacts;
import com.purbon.kafka.topology.model.artefact.KsqlStreamArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlTableArtefact;
import com.purbon.kafka.topology.utils.Either;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KSqlArtefactManager extends ArtefactManager {

  private static final Logger LOGGER = LogManager.getLogger(KSqlArtefactManager.class);

  public KSqlArtefactManager(
      ArtefactClient client, Configuration config, String topologyFileOrDir) {
    super(client, config, topologyFileOrDir);
  }

  public KSqlArtefactManager(
      Map<String, ? extends ArtefactClient> clients,
      Configuration config,
      String topologyFileOrDir) {
    super(clients, config, topologyFileOrDir);
  }

  @Override
  Collection<? extends Artefact> loadActualClusterStateIfAvailable(ExecutionPlan plan)
      throws IOException {
    return config.fetchStateFromTheCluster() ? getClustersState() : plan.getKSqlArtefacts();
  }

  private Collection<? extends Artefact> getClustersState() throws IOException {
    List<Either> list =
        clients.values().stream()
            .map(
                client -> {
                  try {
                    Collection<? extends Artefact> artefacts = client.getClusterState();
                    if (artefacts.isEmpty()) {
                      return Either.Right(null);
                    }
                    return Either.Right(artefacts);
                  } catch (IOException ex) {
                    return Either.Left(ex);
                  }
                })
            .collect(Collectors.toList());

    List<IOException> errors =
        list.stream()
            .filter(Either::isLeft)
            .map(e -> (IOException) e.getLeft().get())
            .collect(Collectors.toList());
    if (errors.size() > 0) {
      throw new IOException(errors.get(0));
    }

    return list.stream()
        .filter(Either::isRight)
        .flatMap(
            (Function<Either, Stream<? extends Artefact>>)
                either -> {
                  Collection<? extends Artefact> artefacts =
                      (Collection<? extends Artefact>) either.getRight().get();
                  return artefacts.stream();
                })
        .map(
            artefact -> {
              if (artefact instanceof KsqlStreamArtefact) {
                return new KsqlStreamArtefact(artefact.getPath(), null, artefact.getName());
              } else if (artefact instanceof KsqlTableArtefact) {
                return new KsqlTableArtefact(artefact.getPath(), null, artefact.getName());
              } else {
                LOGGER.error("KSQL Artefact of wrong type " + artefact.getClass());
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  @Override
  Set<Artefact> parseNewArtefacts(Topology topology) {
    return topology.getProjects().stream()
        .flatMap(
            (Function<Project, Stream<Artefact>>)
                project -> {
                  KsqlArtefacts kSql = project.getKsqlArtefacts();
                  return Stream.concat(kSql.getStreams().stream(), kSql.getTables().stream());
                })
        .collect(Collectors.toSet());
  }

  @Override
  boolean isAllowDelete() {
    return config.isAllowDeleteKsqlArtefacts();
  }

  @Override
  String rootPath() {
    return Files.isDirectory(Paths.get(topologyFileOrDir))
        ? topologyFileOrDir
        : new File(topologyFileOrDir).getParent();
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of KSQL Artifacts:");
    getClustersState().forEach(out::println);
  }
}
