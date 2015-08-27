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
package org.kitesdk.apps.streaming;

import org.apache.hadoop.conf.Configured;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.JobContext;

public class MockStreamingJob extends AbstractStreamingJob{

  public void run(@DataIn(name = "mock_input") Object input,
                  @DataOut(name = "mock_output") Object output) {

    // Mock implementation does nothing.
  }
}
