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
import java.lang.reflect.Field;
import java.util.Vector;

public class SQLSmithLoader {

  private static final NativeLibLoaderImpl loaderForSQLSmith = new NativeLibLoaderImpl("sqlsmith");

  private static SQLSmithNative sqlSmithNative = null;

  public static synchronized boolean isLoaded() {
    return sqlSmithNative != null;
  }

  public static synchronized SQLSmithNative loadApi() throws RuntimeException {
    if (sqlSmithNative != null) {
      return sqlSmithNative;
    }
    String path = loaderForSQLSmith.loadNativeLibFromJar().getAbsolutePath();
    System.load(path);
//    System.out.println(path);
//    System.exit(0);
//    System.load("/home/ahmad/Documents/project/fuzz-testing-for-spark/src/main/resources/lib/Linux/aarch64/libsqlsmith.so");
    sqlSmithNative = new SQLSmithNative();
    return sqlSmithNative;
  }
}