package org.kitesdk.apps;

import org.junit.After;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;

/**
 * Base class for testing Kite applications over Hive.
 */
public abstract class MiniAppTest extends MiniDFSTest {

  @After
  public void clearDatasets() {

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive");

    for (String namespace: repo.namespaces()) {
      for (String name: repo.datasets(namespace)) {

        repo.delete(namespace, name);
      }
    }
  }
}
