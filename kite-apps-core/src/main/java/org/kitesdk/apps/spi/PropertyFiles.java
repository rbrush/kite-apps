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
package org.kitesdk.apps.spi;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.apps.AppException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Helper class for working with property files.
 */
public class PropertyFiles {

  public static Map<String,String> load(File input) {

    FileInputStream stream = null;

    try {
      stream = new FileInputStream(input);
      return load(stream);
    } catch (FileNotFoundException e) {
      throw new AppException(e);
    } finally {
      Closeables.closeQuietly(stream);
    }
  }

  public static Map<String,String> load(FileSystem fs, Path path) {

    InputStream stream = null;

    try {
      stream = fs.open(path);
      return load(stream);
    } catch (FileNotFoundException e) {
      throw new AppException(e);
    } catch (IOException e) {
      throw new AppException(e);
    } finally {
      Closeables.closeQuietly(stream);
    }
  }


  private static Map<String,String> load(InputStream input) {


    Map<String,String> settings = Maps.newHashMap();

    try {

      Properties props = new Properties();

      props.load(input);

      for (String propName: props.stringPropertyNames()) {
        settings.put(propName, props.getProperty(propName));
      }

    } catch (FileNotFoundException e) {
      throw new AppException(e);
    } catch (IOException e) {
      throw new AppException(e);
    }
    return settings;
  }

  public static Map<String, String> loadIfExists(FileSystem fileSystem, Path path) {

    try {

      return fileSystem.exists(path) ?
          load(fileSystem, path) :
          Collections.<String,String>emptyMap();

    } catch (IOException e) {
      throw new AppException(e);
    }
  }
}
