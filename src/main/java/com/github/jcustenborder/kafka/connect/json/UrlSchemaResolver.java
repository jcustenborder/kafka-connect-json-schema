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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.everit.json.schema.Schema;

import java.net.URL;

public class UrlSchemaResolver<R extends ConnectRecord<R>> implements SchemaResolver<R> {
  private final Schema schema;

  protected UrlSchemaResolver(URL schemaUrl, SchemaDownloader schemaDownloader) {
    this.schema = schemaDownloader.downloadSchema(schemaUrl);
  }

  @Override
  public Schema resolveSchema(R record, org.apache.kafka.connect.data.Schema inputSchema, JsonNode jsonNode) {
    return schema;
  }
}
