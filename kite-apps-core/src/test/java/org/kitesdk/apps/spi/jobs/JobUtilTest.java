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
package org.kitesdk.apps.spi.jobs;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.event.StandardEvent;


import java.util.List;

public class JobUtilTest {

  @Test
  public void testGetSchemas() {

    List<Schema> schemas = JobUtil.getSchemas(new StandardEventsJob());

    Assert.assertEquals(1, schemas.size());

    Assert.assertEquals(SpecificData.get().getSchema(StandardEvent.class), schemas.get(0));
  }

}
