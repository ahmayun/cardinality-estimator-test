/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlsmith.loader;

import java.io.File;
import java.util.UUID;

public class Utils {

  public static File createTempDir() {
    final String tempDir = "fuzz-testing-" + UUID.randomUUID();
    File sysTmpDir = new File(System.getProperty("java.io.tmpdir"));
    File tmpDir = new File(sysTmpDir, tempDir);
    if (tmpDir.mkdir()) {
      tmpDir.deleteOnExit();
      return tmpDir;
    } else {
      // If we cannot make a temporary dir, just returns
      // the system one (e.g., `/tmp`).
      return sysTmpDir;
    }
  }
}
