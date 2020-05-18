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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class FromJsonSchemaConverterTest {
  private static final Logger log = LoggerFactory.getLogger(FromJsonSchemaConverterTest.class);

  JsonConfig config;
  FromJsonSchemaConverterFactory factory;

  @BeforeEach
  public void before() {
    config = new FromJsonConfig(ImmutableMap.of(
        FromJsonConfig.SCHEMA_INLINE_CONF, "\"string\"",
        FromJsonConfig.SCHEMA_LOCATION_CONF, JsonConfig.SchemaLocation.Inline.toString(),
        JsonConfig.EXCLUDE_LOCATIONS_CONF, "#/properties/log_params"
    ));
    this.factory = new FromJsonSchemaConverterFactory(config);
  }


  org.everit.json.schema.Schema jsonSchema(String type) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    return TestUtils.jsonSchema(rawSchema);
  }

  org.everit.json.schema.Schema jsonSchema(String type, String key1, String value1) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    rawSchema.put(key1, value1);
    return TestUtils.jsonSchema(rawSchema);
  }

  org.everit.json.schema.Schema jsonSchema(String type, String key1, String value1, String key2, String value2) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    rawSchema.put(key1, value1);
    rawSchema.put(key2, value2);
    return TestUtils.jsonSchema(rawSchema);
  }

  void assertJsonSchema(org.apache.kafka.connect.data.Schema expected, org.everit.json.schema.Schema input) {
    FromJsonState state = this.factory.fromJSON(input);
    assertSchema(expected, state.schema);
  }

  @Test
  public void booleanSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("boolean");
    assertJsonSchema(Schema.BOOLEAN_SCHEMA, jsonSchema);
  }

  @Test
  public void stringSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("string");
    assertJsonSchema(Schema.STRING_SCHEMA, jsonSchema);
  }

  @Test
  public void integerSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("integer");
    assertJsonSchema(Schema.INT64_SCHEMA, jsonSchema);
  }

  @Test
  public void numberSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("number");
    assertJsonSchema(Schema.FLOAT64_SCHEMA, jsonSchema);
  }

  @Test
  public void dateSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("string", "format", "date");
    assertJsonSchema(Date.SCHEMA, jsonSchema);
  }

  @Test
  public void timeSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("string", "format", "time");
    assertJsonSchema(Time.SCHEMA, jsonSchema);
  }

  @Test
  public void datetimeSchema() {
    org.everit.json.schema.Schema jsonSchema = jsonSchema("string", "format", "date-time");
    assertJsonSchema(Timestamp.SCHEMA, jsonSchema);
  }

  org.everit.json.schema.Schema loadSchema(String name) throws IOException {
    try (InputStream inputStream = this.getClass().getResourceAsStream(name)) {
      JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
      return SchemaLoader.builder()
          .draftV7Support()
          .addFormatValidator(new DateFormatValidator())
          .addFormatValidator(new TimeFormatValidator())
          .addFormatValidator(new DateTimeFormatValidator())
          .schemaJson(rawSchema)
          .build()
          .load()
          .build();
    }
  }

  @Test
  public void productSchema() throws IOException {
    org.everit.json.schema.Schema jsonSchema = loadSchema("SchemaConverterTest/product.schema.json");
    Schema expected = SchemaBuilder.struct()
        .name("Product")
        .doc("A product from Acme's catalog")
        .field("price", SchemaBuilder.float64().doc("The price of the product").build())
        .field("productId", SchemaBuilder.int64().doc("The unique identifier for a product").build())
        .field("productName", SchemaBuilder.string().doc("Name of the product").build())
        .build();
    assertJsonSchema(expected, jsonSchema);
  }

  @Test
  public void wikiMediaRecentChangeSchema() throws IOException {
    org.everit.json.schema.Schema jsonSchema = loadSchema("SchemaConverterTest/wikimedia.recentchange.schema.json");
    Schema expected = SchemaBuilder.struct()
        .name("mediawiki.recentchange")
        .doc("Represents a MW RecentChange event. https://www.mediawiki.org/wiki/Manual:RCFeed\n")
        .field("price", SchemaBuilder.float64().doc("The price of the product").build())
        .field("productId", SchemaBuilder.int64().doc("The unique identifier for a product").build())
        .field("productName", SchemaBuilder.string().doc("Name of the product").build())
        .build();
    assertNotNull(expected);
  }

  @Test
  public void nested() throws IOException {
    org.everit.json.schema.Schema jsonSchema = loadSchema("SchemaConverterTest/nested.schema.json");
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .optional()
        .field("city", SchemaBuilder.string().build())
        .field("state", SchemaBuilder.string().build())
        .field("street_address", SchemaBuilder.string().build())
        .build();
    Schema expected = SchemaBuilder.struct()
        .name("Customer")
        .field("billing_address", addressSchema)
        .field("shipping_address", addressSchema)
        .build();
    assertJsonSchema(expected, jsonSchema);
  }

  @Test
  public void array() {
    JSONObject rawSchema = new JSONObject()
        .put("type", "array")
        .put("items", new JSONObject().put("type", "number"));
    org.everit.json.schema.Schema jsonSchema = TestUtils.jsonSchema(rawSchema);
    assertJsonSchema(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), jsonSchema);
  }

  @TestFactory
  public Stream<DynamicTest> stringFormats() {
    Map<String, String> formats = new LinkedHashMap<>();
    formats.put("email", "test@example.com");
    formats.put("idn-email", "test@example.com");
    formats.put("hostname", "example.com");
    formats.put("idn-hostname", "example.com");
    formats.put("ipv4", "127.0.0.1");
    formats.put("ipv6", "::1");
    formats.put("uri", "http://example.com");
    formats.put("uri-reference", "http://example.com");
    formats.put("iri", "http://example.com");
    formats.put("iri-reference", "http://example.com");
    formats.put("uri-template", "http://example.com/~{username}/");

    return formats.entrySet()
        .stream()
        .map(e -> dynamicTest(e.getKey(), () -> {
          JSONObject rawSchema = new JSONObject()
              .put("type", "string")
              .put("format", e.getKey());
          log.trace("schema = '{}'", rawSchema);
          org.everit.json.schema.Schema jsonSchema = TestUtils.jsonSchema(rawSchema);
          assertJsonSchema(Schema.STRING_SCHEMA, jsonSchema);
        }));
  }

}
