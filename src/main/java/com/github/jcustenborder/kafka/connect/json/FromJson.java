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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

@Title("From Json transformation")
@Description("The FromJson will read JSON data that is in string on byte form and parse the data to " +
    "a connect structure based on the JSON schema provided.")
@DocumentationTip("This transformation expects data to be in either String or Byte format. You are " +
    "most likely going to use the ByteArrayConverter or the StringConverter.")
public class FromJson<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(FromJson.class);
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


  void validateJson(JSONObject jsonObject) {
    try {
      this.fromJsonState.jsonSchema.validate(jsonObject);
    } catch (ValidationException ex) {
      StringBuilder builder = new StringBuilder();
      builder.append(
          String.format(
              "Could not validate JSON. Found %s violations(s).",
              ex.getViolationCount()
          )
      );
      for (ValidationException message : ex.getCausingExceptions()) {
        log.error("Validation exception", message);
        builder.append("\n");
        builder.append(message.getMessage());
      }
      throw new DataException(
          builder.toString(),
          ex
      );
    }
  }


  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    try {
      if (this.config.validateJson) {
        try (InputStream inputStream = new ByteArrayInputStream(input)) {
          JSONObject jsonObject = Utils.loadObject(inputStream);
          validateJson(jsonObject);
        }
      }
      JsonNode node = this.objectMapper.readValue(input, JsonNode.class);
      return processJsonNode(record, inputSchema, node);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    try {
      if (this.config.validateJson) {
        try (Reader reader = new StringReader(input)) {
          JSONObject jsonObject = Utils.loadObject(reader);
          validateJson(jsonObject);
        }
      }
      JsonNode node = this.objectMapper.readValue(input, JsonNode.class);
      return processJsonNode(record, inputSchema, node);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  FromJsonState fromJsonState;
  FromJsonSchemaConverterFactory fromJsonSchemaConverterFactory;
  ObjectMapper objectMapper;

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new FromJsonConfig(map);
    this.fromJsonSchemaConverterFactory = new FromJsonSchemaConverterFactory(config);

    org.everit.json.schema.Schema schema;
    if (JsonConfig.SchemaLocation.Url == this.config.schemaLocation) {
      try {
        try (InputStream inputStream = this.config.schemaUrl.openStream()) {
          schema = Utils.loadSchema(inputStream);
        }
      } catch (IOException e) {
        ConfigException exception = new ConfigException(JsonConfig.SCHEMA_URL_CONF, this.config.schemaUrl, "exception while loading schema");
        exception.initCause(e);
        throw exception;
      }
    } else if (JsonConfig.SchemaLocation.Inline == this.config.schemaLocation) {
      schema = Utils.loadSchema(this.config.schemaText);
    } else {
      throw new ConfigException(
          JsonConfig.SCHEMA_LOCATION_CONF,
          this.config.schemaLocation.toString(),
          "Location is not supported"
      );
    }

    this.fromJsonState = this.fromJsonSchemaConverterFactory.fromJSON(schema);
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
