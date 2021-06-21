package com.purbon.kafka.topology.model.artefact;

import static org.junit.Assert.*;

import org.junit.Test;

public class KsqlStreamArtefactTest {

  @Test
  public void testLabelNotPrintedWhenMissing() {
    KsqlStreamArtefact artefact = new KsqlStreamArtefact("file.ksql", null, "STREAM_NAME");
    assertEquals("STREAM STREAM_NAME", artefact.toString());
  }

  @Test
  public void testLabelPrintedWhenPresent() {
    KsqlStreamArtefact artefact = new KsqlStreamArtefact("file.ksql", "zone-a", "STREAM_NAME");
    assertEquals("STREAM STREAM_NAME (zone-a)", artefact.toString());
  }
}
