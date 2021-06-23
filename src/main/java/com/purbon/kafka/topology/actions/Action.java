package com.purbon.kafka.topology.actions;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public interface Action {

  void run() throws IOException;

  default void dryRun() throws IOException {
    System.out.printf("No dryRun check specified for %s", this.getClass().getName());
  };

  default List<TopologyAclBinding> getBindings() {
    return Collections.emptyList();
  }
}
