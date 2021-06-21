package com.purbon.kafka.topology.model.artefact;

import static org.junit.Assert.*;

import org.junit.Test;

public class KsqlTableArtefactTest {

  @Test
  public void testLabelNotPrintedWhenMissing() {
    KsqlTableArtefact artefact = new KsqlTableArtefact("file.ksql", null, "TABLE_NAME");
    assertEquals("TABLE TABLE_NAME", artefact.toString());
  }

  @Test
  public void testLabelPrintedWhenPresent() {
    KsqlTableArtefact artefact = new KsqlTableArtefact("file.ksql", "zone-a", "TABLE_NAME");
    assertEquals("TABLE TABLE_NAME (zone-a)", artefact.toString());
  }
}
