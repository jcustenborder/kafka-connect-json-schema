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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaConverterTest {

  org.everit.json.schema.Schema jsonSchema(JSONObject rawSchema) {
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

  org.everit.json.schema.Schema jsonSchema(String type) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    return jsonSchema(rawSchema);
  }

  org.everit.json.schema.Schema jsonSchema(String type, String key1, String value1) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    rawSchema.put(key1, value1);
    return jsonSchema(rawSchema);
  }

  org.everit.json.schema.Schema jsonSchema(String type, String key1, String value1, String key2, String value2) {
    JSONObject rawSchema = new JSONObject();
    rawSchema.put("type", type);
    rawSchema.put(key1, value1);
    rawSchema.put(key2, value2);
    return jsonSchema(rawSchema);
  }

  void assertJsonSchema(org.apache.kafka.connect.data.Schema expected, org.everit.json.schema.Schema input) {
    org.apache.kafka.connect.data.Schema actual = SchemaConverter.fromJSON(input);
    assertSchema(expected, actual);
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
    org.everit.json.schema.Schema jsonSchema = jsonSchema(rawSchema);
    assertJsonSchema(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), jsonSchema);
  }


}
