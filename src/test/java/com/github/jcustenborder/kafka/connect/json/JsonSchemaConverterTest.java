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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonSchemaConverterTest {
  private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverterTest.class);
  JsonSchemaConverter converter;

  @BeforeEach
  public void beforeEach() {
    this.converter = new JsonSchemaConverter();
  }

  @Test
  public void nulls() {
    this.converter.configure(
        ImmutableMap.of(),
        false
    );
    SchemaAndValue expected = SchemaAndValue.NULL;
    Headers headers = new RecordHeaders();
    byte[] buffer = this.converter.fromConnectData("topic", headers, expected.schema(), expected.value());
    assertNull(buffer, "buffer should be null.");
  }

  @Test
  public void roundTripString() {
    this.converter.configure(
        ImmutableMap.of(),
        false
    );
    SchemaAndValue expected = new SchemaAndValue(
        Schema.STRING_SCHEMA,
        "This is a test"
    );
    Headers headers = new RecordHeaders();
    byte[] buffer = this.converter.fromConnectData("topic", headers, expected.schema(), expected.value());
    assertNotNull(buffer, "buffer should not be null.");
    assertTrue(buffer.length > 0, "buffer should be longer than zero.");
    Header schemaHeader = headers.lastHeader(this.converter.jsonSchemaHeader);
    assertNotNull(schemaHeader, "schemaHeader should not be null.");
    SchemaAndValue actual = this.converter.toConnectData("topic", headers, buffer);
    assertNotNull(actual, "actual should not be null.");
    assertSchema(expected.schema(), actual.schema());
    assertEquals(expected.value(), actual.value());
  }

  @Test
  public void nested() throws IOException {
    this.converter.configure(
        ImmutableMap.of(),
        false
    );
    Schema addressSchema = SchemaBuilder.struct()
        .name("Address")
        .optional()
        .field("city", SchemaBuilder.string().build())
        .field("state", SchemaBuilder.string().build())
        .field("street_address", SchemaBuilder.string().build())
        .build();
    Schema customer = SchemaBuilder.struct()
        .name("Customer")
        .field("billing_address", addressSchema)
        .field("shipping_address", addressSchema)
        .build();
    Struct billingAddress = new Struct(addressSchema)
        .put("city", "Austin")
        .put("state", "TX")
        .put("street_address", "123 Main St");
    Struct shippingAddress = new Struct(addressSchema)
        .put("city", "Dallas")
        .put("state", "TX")
        .put("street_address", "321 Something St");
    Struct struct = new Struct(customer)
        .put("billing_address", billingAddress)
        .put("shipping_address", shippingAddress);
    SchemaAndValue expected = new SchemaAndValue(customer, struct);
    Headers headers = new RecordHeaders();
    byte[] buffer = this.converter.fromConnectData("topic", headers, expected.schema(), expected.value());
    log.trace(new String(buffer, Charsets.UTF_8));
    assertNotNull(buffer, "buffer should not be null.");
    assertTrue(buffer.length > 0, "buffer should be longer than zero.");
    Header schemaHeader = headers.lastHeader(this.converter.jsonSchemaHeader);
    assertNotNull(schemaHeader, "schemaHeader should not be null.");
    SchemaAndValue actual = this.converter.toConnectData("topic", headers, buffer);
    assertNotNull(actual, "actual should not be null.");
    assertSchema(expected.schema(), actual.schema());
    assertEquals(expected.value(), actual.value());
  }

}
