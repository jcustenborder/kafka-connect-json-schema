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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Title("From Json transformation")
@Description("The FromJson will read JSON data that is in string on byte form and parse the data to " +
    "a connect structure based on the JSON schema provided.")
public class FromJson<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  FromJsonConfig config;

  protected FromJson(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return FromJsonConfig.config();
  }

  @Override
  public void close() {

  }

  SchemaAndValue processJsonNode(R record, Schema inputSchema, JsonNode node) {
    Object result = this.fromJsonState.visitor.visit(node);
    return new SchemaAndValue(this.fromJsonState.schema, result);
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    try {
      JsonNode node = this.objectMapper.readValue(input, JsonNode.class);
      return processJsonNode(record, inputSchema, node);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    try {
      JsonNode node = this.objectMapper.readValue(input, JsonNode.class);
      return processJsonNode(record, inputSchema, node);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  FromJsonState fromJsonState;
  ObjectMapper objectMapper;

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new FromJsonConfig(map);

    org.everit.json.schema.Schema schema;
    try {
      try (InputStream inputStream = this.config.schemaLocation.openStream()) {
        schema = Utils.loadSchema(inputStream);
      }
    } catch (IOException e) {
      ConfigException exception = new ConfigException(FromJsonConfig.SCHEMA_LOCATION_CONF, this.config.schemaLocation, "exception while loading schema");
      exception.initCause(e);
      throw exception;
    }

    this.fromJsonState = FromJsonSchemaConverter.fromJSON(schema);
    this.objectMapper = JacksonFactory.create();
  }

  public static class Key<R extends ConnectRecord<R>> extends FromJson<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends FromJson<R> {
    public Value() {
      super(false);
    }
  }
}
