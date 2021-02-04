/**
 * Copyright © 2020 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.json;

import org.apache.kafka.common.config.ConfigException;
import org.everit.json.schema.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class DefaultSchemaDownloader implements SchemaDownloader {
  @Override
  public Schema downloadSchema(URL schemaUrl) {
    try {
      try (InputStream inputStream = schemaUrl.openStream()) {
        return Utils.loadSchema(inputStream);
      }
    } catch (IOException e) {
      ConfigException exception = new ConfigException(JsonConfig.SCHEMA_URL_CONF, schemaUrl, "error downloading schema");
      exception.initCause(e);
      throw exception;
    }
  }
}
