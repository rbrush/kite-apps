/**
 * Copyright 2015 Cerner Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.apps.spark.spi;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spark.apps.SimpleSparkApp;
import org.kitesdk.apps.spark.apps.SimpleSparkJob;
import org.kitesdk.apps.spark.spi.scheduled.SparkScheduledJobMain;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.apps.spi.oozie.XMLUtil;
import org.w3c.dom.Document;

import javax.xml.xpath.XPath;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test scheduling of a Spark job.
 */
public class OozieSparkSchedulingTest {

  Schedule testSchedule;

  @Before
  public void createTestSchedule() {

    testSchedule = new Schedule.Builder().jobClass(SimpleSparkJob.class)
        .frequency("0 * * * *")
        .withView("source.users", SimpleSparkApp.INPUT_URI_PATTERN, 1)
        .withView("target.users", SimpleSparkApp.OUTPUT_URI_PATTERN, 1)
        .build();
  }

  @Test
  public void testWriteWorkflow() throws Exception {

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    OozieScheduling.writeWorkFlow(testSchedule, new Configuration(), output);

    Document workflow = XMLUtil.toDom(output);

    XPath xpath = XMLUtil.getXPath();

    assertEquals(testSchedule.getName(),
        xpath.evaluate("wf:workflow-app/@name", workflow));

    // The spark class should be invoked with the argument specifying the job.
    assertEquals(SparkScheduledJobMain.class.getName(),
        xpath.evaluate("wf:workflow-app/wf:action/sp:spark/sp:class", workflow));

    assertEquals(SimpleSparkJob.class.getName(),
        xpath.evaluate("wf:workflow-app/wf:action/sp:spark/sp:arg", workflow));

    String sparkOptions = xpath.evaluate("wf:workflow-app/wf:action/sp:spark/sp:spark-opts/text()", workflow);

    assertTrue(sparkOptions.contains("${coordNominalTime}"));

    assertTrue(sparkOptions.contains("${coord_source_users}"));

    assertTrue(sparkOptions.contains("${coord_target_users}"));
  }
}
