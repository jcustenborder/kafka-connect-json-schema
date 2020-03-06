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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class JsonSchemaConverter implements Converter {
  private static final String KEY_HEADER = "json.key.schema";
  private static final String VALUE_HEADER = "json.value.schema";
  JsonSchemaConverterConfig config;
  String jsonSchemaHeader;
  Charset encodingCharset;
  ObjectMapper objectMapper;
  Map<Schema, FromConnectState> fromConnectStateLookup = new HashMap<>();
  Map<String, FromJsonState> toConnectStateLookup = new HashMap<>();
  Header fallbackHeader;

  @Override
  public void configure(Map<String, ?> settings, boolean isKey) {
    this.config = new JsonSchemaConverterConfig(settings);
    this.jsonSchemaHeader = isKey ? KEY_HEADER : VALUE_HEADER;
    this.encodingCharset = Charsets.UTF_8;
    this.objectMapper = JacksonFactory.create();

    if (this.config.insertSchema) {
      byte[] headerValue;
      if (FromJsonConfig.SchemaLocation.Url == this.config.schemaLocation) {
        try {
          try (InputStream inputStream = this.config.schemaUrl.openStream()) {
            headerValue = ByteStreams.toByteArray(inputStream);
          }
        } catch (IOException e) {
          ConfigException exception = new ConfigException(FromJsonConfig.SCHEMA_URL_CONF, this.config.schemaUrl, "exception while loading schema");
          exception.initCause(e);
          throw exception;
        }
      } else if (FromJsonConfig.SchemaLocation.Inline == this.config.schemaLocation) {
        headerValue = this.jsonSchemaHeader.getBytes(Charsets.UTF_8);
      } else {
        throw new ConfigException(
            FromJsonConfig.SCHEMA_LOCATION_CONF,
            this.config.schemaLocation.toString(),
            "Location is not supported"
        );
      }
      this.fallbackHeader = new RecordHeader(this.jsonSchemaHeader, headerValue);
    } else {
      fallbackHeader = null;
    }
  }

  @Override
  public byte[] fromConnectData(String s, Schema schema, Object o) {
    throw new UnsupportedOperationException(
        "This converter requires Kafka 2.4.0 or higher with header support."
    );
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (null == value) {
      return null;
    }


    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      try (JsonGenerator jsonGenerator = objectMapper.getFactory().createGenerator(outputStream)) {
        FromConnectState fromConnectState = fromConnectStateLookup.computeIfAbsent(schema, s -> FromConnectSchemaConverter.toJsonSchema(schema, jsonSchemaHeader));
        headers.add(fromConnectState.header);
        fromConnectState.visitor.doVisit(jsonGenerator, value);
      }
      return outputStream.toByteArray();
    } catch (IOException ex) {
      throw new SerializationException(ex);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String s, byte[] bytes) {
    throw new UnsupportedOperationException(
        "This converter requires Kafka 2.4.0 or higher with header support."
    );
  }

  Header schemaHeader(Headers headers) {
    Header schemaHeader = headers.lastHeader(this.jsonSchemaHeader);
    if (null == schemaHeader) {
      schemaHeader = this.fallbackHeader;
    }
    return schemaHeader;
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    if (null == value) {
      return SchemaAndValue.NULL;
    }

    final Header schemaHeader = schemaHeader(headers);

    if (null == schemaHeader) {
      throw new DataException(
          String.format(
              "Record does not have '{}' header and '%s' is not enabled.",
              this.jsonSchemaHeader,
              JsonSchemaConverterConfig.INSERT_SCHEMA_ENABLED_CONF
          )
      );
    }

    String hash = Hashing.goodFastHash(32)
        .hashBytes(schemaHeader.value())
        .toString();
    FromJsonState state = this.toConnectStateLookup.computeIfAbsent(hash, h -> {
      org.everit.json.schema.Schema schema = Utils.loadSchema(schemaHeader);
      return FromJsonSchemaConverter.fromJSON(schema);
    });

    JsonNode jsonNode;
    try {
      jsonNode = this.objectMapper.readValue(value, JsonNode.class);
    } catch (IOException ex) {
      throw new SerializationException(ex);
    }
    Object result = state.visitor.visit(jsonNode);
    return new SchemaAndValue(state.schema, result);
  }
}
