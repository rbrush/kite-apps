package org.kitesdk.apps.spi.oozie;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by rb4106 on 5/11/15.
 */
public class URIShimTest {

  @Test
  public void testHDFSToKite() {
    String kiteURI = URIShim.HDFSToKite("hdfs:///user/hive/warehouse/example.db/events/year=2015/month=05/day=11/hour=20/minute=11");

    Assert.assertEquals("view:hive:example/events?year=2015&month=05&day=11&hour=20&minute=11", kiteURI);
  }

  @Test
  public void testKiteToHDFS() {
    String hdfsURI = URIShim.kiteToHDFS("dataset:hive:example/events?year=2015&month=05&day=11&hour=20&minute=11");

    Assert.assertEquals("hdfs:///user/hive/warehouse/example.db/events/year=2015/month=05/day=11/hour=20/minute=11", hdfsURI);
  }
}
