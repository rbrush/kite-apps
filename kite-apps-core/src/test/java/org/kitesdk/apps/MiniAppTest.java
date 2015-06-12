package org.kitesdk.apps;

import org.junit.After;
import org.junit.Before;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.hive.MetaStoreUtil;

/**
 * Base class for testing Kite applications over Hive.
 */
public abstract class MiniAppTest extends MiniDFSTest {

  @Before
  public void setupConf() {
    DefaultConfiguration.set(getConfiguration());
  }

  @Before
  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(getConfiguration());
    for (String database : metastore.getAllDatabases()) {
      for (String table : metastore.getAllTables(database)) {
        metastore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        metastore.dropDatabase(database, true);
      }
    }
  }
}
