package com.purbon.kafka.topology.actions;

import static com.purbon.kafka.topology.utils.Utils.filePath;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.utils.Utils;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateArtefactAction extends BaseAction {

  private static final Logger LOGGER = LogManager.getLogger(CreateArtefactAction.class);

  private final ArtefactClient client;
  private final Artefact artefact;
  private final String rootPath;
  private final Collection<? extends Artefact> artefacts;

  public CreateArtefactAction(
      ArtefactClient client,
      String rootPath,
      Collection<? extends Artefact> artefacts,
      Artefact artefact) {
    this.client = client;
    this.artefact = artefact;
    this.artefacts = artefacts;
    this.rootPath = rootPath;
  }

  @Override
  public void run() throws IOException {
    if (!artefacts.contains(artefact)) {
      LOGGER.info(
          String.format(
              "Creating artefact %s for client %s", artefact.getName(), client.getClass()));
      client.add(content());
    }
  }

  public Artefact getArtefact() {
    return artefact;
  }

  private String content() throws IOException {
    LOGGER.debug(
        "Reading artefact content from " + artefact.getPath() + " with rootPath " + rootPath);
    return Utils.readFullFile(filePath(artefact.getPath(), rootPath));
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Artefact", artefact.getPath());
    return map;
  }
}
