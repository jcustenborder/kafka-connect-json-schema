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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.json.JSONObject;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class FromConnectSchemaConverterTest {
  private static final Logger log = LoggerFactory.getLogger(FromConnectSchemaConverterTest.class);

  @Test
  public void array() {
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .optional()
        .field("city", SchemaBuilder.string().build())
        .field("state", SchemaBuilder.string().build())
        .field("street_address", SchemaBuilder.string().build())
        .build();
    Schema arraySchema = SchemaBuilder.array(addressSchema);
    Schema expected = SchemaBuilder.struct()
        .name("Person")
        .field("previous_addresses", arraySchema)
        .build();
    FromConnectState state = FromConnectSchemaConverter.toJsonSchema(expected, "foo");
    assertNotNull(state, "rawJsonSchema should not be null.");
    JSONObject rawJsonSchema = HeaderUtils.loadObject(state.header);
    log.trace("rawJsonSchema = {}", rawJsonSchema.toString(2));
    org.everit.json.schema.Schema jsonSchema = TestUtils.jsonSchema(rawJsonSchema);
    assertNotNull(jsonSchema);
  }

  @Test
  public void struct() {
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .doc("An object to store an address.")
        .optional()
        .field("city", SchemaBuilder.string().doc("city of the address.").build())
        .field("state", SchemaBuilder.string().doc("state of the address.").build())
        .field("street_address", SchemaBuilder.string().doc("street address of the address.").build())
        .build();
    Schema expected = SchemaBuilder.struct()
        .name("Customer")
        .field("first_name", SchemaBuilder.string().doc("First name of the customer").build())
        .field("billing_address", addressSchema)
        .field("shipping_address", addressSchema)
        .build();

    FromConnectState state = FromConnectSchemaConverter.toJsonSchema(expected, "foo");
    assertNotNull(state, "rawJsonSchema should not be null.");
    JSONObject rawJsonSchema = HeaderUtils.loadObject(state.header);
    log.trace("rawJsonSchema = {}", rawJsonSchema.toString(2));
    org.everit.json.schema.Schema jsonSchema = TestUtils.jsonSchema(rawJsonSchema);
    assertNotNull(jsonSchema);
  }

  @TestFactory
  public Stream<DynamicTest> primitives() {
    return FromConnectSchemaConverter.PRIMITIVE_TYPES.entrySet()
        .stream()
        .map(e -> dynamicTest(e.getKey().toString(), () -> {
          String description = String.format("This schema represents a %s", e.getKey());
          Schema expected = SchemaBuilder.type(e.getKey())
              .doc(description)
              .build();
          FromConnectState state = FromConnectSchemaConverter.toJsonSchema(expected, "foo");
          assertNotNull(state, "rawJsonSchema should not be null.");
          JSONObject rawJsonSchema = HeaderUtils.loadObject(state.header);
          assertNotNull(rawJsonSchema, "rawJsonSchema should not be null.");
          log.trace("rawJsonSchema = {}", rawJsonSchema.toString(2));
          e.getValue().forEach((propertyName, expectedValue) -> {
            Object propertyValue = rawJsonSchema.get(propertyName);
            assertEquals(expectedValue, propertyValue);
          });
          assertEquals(description, rawJsonSchema.getString("description"));
          org.everit.json.schema.Schema jsonSchema = TestUtils.jsonSchema(rawJsonSchema);

        }));
  }


}
