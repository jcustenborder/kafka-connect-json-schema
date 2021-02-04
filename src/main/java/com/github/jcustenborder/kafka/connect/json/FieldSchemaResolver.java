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
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.Schema;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class FieldSchemaResolver<R extends ConnectRecord<R>> implements SchemaResolver<R> {
  public static final String SCHEMA_ID_TEMPLATE = "{schema_id}";
  private final String fieldPath;
  private final String[] fieldNames;
  private final String urlTemplate;
  private final SchemaDownloader schemaDownloader;
  private final Map<String, Schema> schemaCache;

  @SuppressWarnings("unchecked")
  protected FieldSchemaResolver(String fieldPath, String urlTemplate, SchemaDownloader schemaDownloader) {
    this.fieldPath = fieldPath;
    this.fieldNames = fieldPath.split("\\.");
    this.urlTemplate = urlTemplate;
    this.schemaDownloader = schemaDownloader;
    this.schemaCache = new ReferenceMap();
  }

  @Override
  public Schema resolveSchema(R record, org.apache.kafka.connect.data.Schema inputSchema, JsonNode jsonNode) {
    String schemaId = schemaIdFromField(jsonNode);
    return getOrLoadSchema(schemaId);
  }

  String schemaIdFromField(JsonNode jsonNode) {
    JsonNode schemaField = jsonNode;
    for (String field : fieldNames) {
      schemaField = schemaField.path(field);
    }

    String schemaId = schemaField.textValue();
    if (schemaId == null || schemaId.isEmpty()) {
      throw new DataException("Unable to obtain schema ID from field path: " + this.fieldPath);
    }

    return schemaId;
  }

  Schema getOrLoadSchema(String schemaId) {
    Schema schema = schemaCache.get(schemaId);

    if (schema == null) {
      URL schemaUrl = urlForSchema(schemaId);
      schema = this.schemaDownloader.downloadSchema(schemaUrl);
      this.schemaCache.put(schemaId, schema);
    }

    return schema;
  }

  URL urlForSchema(String schemaId) {
    try {
      return new URL(this.urlTemplate.replace(SCHEMA_ID_TEMPLATE, schemaId));
    } catch (MalformedURLException e) {
      ConfigException exception = new ConfigException(JsonConfig.SCHEMA_URL_CONF, schemaId, "invalid schema URL template");
      exception.initCause(e);
      throw exception;
    }
  }
}
