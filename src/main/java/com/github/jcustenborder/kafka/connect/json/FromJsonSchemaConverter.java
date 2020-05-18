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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class FromJsonSchemaConverter<T extends org.everit.json.schema.Schema, J extends JsonNode, V> {
  private static final Logger log = LoggerFactory.getLogger(FromJsonSchemaConverter.class);
  protected final FromJsonSchemaConverterFactory factory;
  protected final JsonConfig config;

  protected FromJsonSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
    this.factory = factory;
    this.config = config;
  }

  protected abstract SchemaBuilder schemaBuilder(T schema);

  protected abstract FromJsonConversionKey key();

  protected abstract FromJsonVisitor<J, V> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors);

  protected abstract void fromJSON(SchemaBuilder builder, T jsonSchema, Map<String, FromJsonVisitor> visitors);

  static class BooleanSchemaConverter extends FromJsonSchemaConverter<BooleanSchema, BooleanNode, Boolean> {
    BooleanSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(BooleanSchema schema) {
      return SchemaBuilder.bool();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(BooleanSchema.class).build();
    }

    @Override
    protected FromJsonVisitor<BooleanNode, Boolean> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.BooleanVisitor(connectSchema);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, BooleanSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {

    }
  }

  static class ObjectSchemaConverter extends FromJsonSchemaConverter<ObjectSchema, ObjectNode, Struct> {

    ObjectSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(ObjectSchema schema) {
      return SchemaBuilder.struct();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(ObjectSchema.class).build();
    }

    @Override
    protected FromJsonVisitor<ObjectNode, Struct> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.StructVisitor(connectSchema, visitors);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, ObjectSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      Set<String> requiredProperties = ImmutableSet.copyOf(jsonSchema.getRequiredProperties());
      jsonSchema.getPropertySchemas()
          .entrySet()
          .stream()
          .filter(e -> {
            String schemaLocation = e.getValue().getSchemaLocation();
            boolean result = !this.config.excludeLocations.contains(schemaLocation);
            log.trace("fromJson() - filtering '{}' location='{}' result = '{}'", e.getKey(), e.getValue().getSchemaLocation(), result);
            return result;
          })
          .sorted(Map.Entry.comparingByKey())
          .forEach(e -> {
            final String propertyName = e.getKey();
            final org.everit.json.schema.Schema propertyJsonSchema = e.getValue();
            final boolean isOptional = !requiredProperties.contains(propertyName);
            log.trace("fromJson() - Processing property '{}' '{}'", propertyName, propertyJsonSchema);
            FromJsonState state = this.factory.fromJSON(propertyJsonSchema, isOptional);
            builder.field(propertyName, state.schema);
            visitors.put(propertyName, state.visitor);
          });
    }
  }

  static class IntegerSchemaConverter extends FromJsonSchemaConverter<NumberSchema, NumericNode, Number> {

    IntegerSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(NumberSchema schema) {
      return SchemaBuilder.int64();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(NumberSchema.class)
          .requiresInteger(true)
          .build();
    }

    @Override
    protected FromJsonVisitor<NumericNode, Number> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.IntegerVisitor(connectSchema);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, NumberSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class FloatSchemaConverter extends FromJsonSchemaConverter<NumberSchema, NumericNode, Number> {

    FloatSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected FromJsonVisitor<NumericNode, Number> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.FloatVisitor(connectSchema);
    }

    @Override
    protected SchemaBuilder schemaBuilder(NumberSchema schema) {
      return SchemaBuilder.float64();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(NumberSchema.class)
          .requiresInteger(false)
          .build();
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, NumberSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class StringSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, String> {

    StringSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return SchemaBuilder.string();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class).build();
    }

    @Override
    protected FromJsonVisitor<TextNode, String> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.StringVisitor(connectSchema);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class DateSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, java.util.Date> {

    DateSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected FromJsonVisitor<TextNode, java.util.Date> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.DateVisitor(connectSchema);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Date.builder();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .format("date")
          .build();
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class TimeSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, java.util.Date> {

    TimeSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Time.builder();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .format("time")
          .build();
    }

    @Override
    protected FromJsonVisitor<TextNode, java.util.Date> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.TimeVisitor(connectSchema);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class DateTimeSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, java.util.Date> {

    DateTimeSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected FromJsonVisitor<TextNode, java.util.Date> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.DateTimeVisitor(connectSchema);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Timestamp.builder();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .format("date-time")
          .build();
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class BytesSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, byte[]> {

    BytesSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return SchemaBuilder.bytes();
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .contentEncoding("base64")
          .build();
    }

    @Override
    protected FromJsonVisitor<TextNode, byte[]> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.BytesVisitor(connectSchema);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {

    }
  }

  static class DecimalSchemaConverter extends FromJsonSchemaConverter<StringSchema, TextNode, Number> {
    public DecimalSchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      int scale = Utils.scale(schema);
      return Decimal.builder(scale);
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .format("decimal")
          .build();
    }

    @Override
    protected FromJsonVisitor<TextNode, Number> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      int scale = Utils.scale(connectSchema);
      return new FromJsonVisitor.DecimalVisitor(connectSchema, scale);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {

    }
  }

  static class ArraySchemaConverter extends FromJsonSchemaConverter<ArraySchema, ArrayNode, List> {

    ArraySchemaConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected SchemaBuilder schemaBuilder(ArraySchema schema) {
      FromJsonState state = this.factory.fromJSON(schema.getAllItemSchema());
      return SchemaBuilder.array(state.schema);
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(ArraySchema.class).build();
    }

    @Override
    protected FromJsonVisitor<ArrayNode, List> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      FromJsonVisitor visitor = visitors.get("item");
      return new FromJsonVisitor.ArrayVisitor(connectSchema, visitor);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, ArraySchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      FromJsonState state = this.factory.fromJSON(jsonSchema.getAllItemSchema());
      visitors.put("item", state.visitor);
    }
  }

  static class CustomTimestampConverter extends FromJsonSchemaConverter<StringSchema, TextNode, java.util.Date> {

    CustomTimestampConverter(FromJsonSchemaConverterFactory factory, JsonConfig config) {
      super(factory, config);
    }

    @Override
    protected FromJsonVisitor<TextNode, java.util.Date> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors) {
      return new FromJsonVisitor.CustomDateVisitor(connectSchema);
    }

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      Object dateTimeFormat = schema.getUnprocessedProperties().get("dateTimeFormat");
      Preconditions.checkNotNull(dateTimeFormat, "dateTimeFormat cannot be null");
      return Timestamp.builder()
          .parameter("dateFormat", dateTimeFormat.toString());
    }

    @Override
    protected FromJsonConversionKey key() {
      return FromJsonConversionKey.from(StringSchema.class)
          .format("custom-timestamp")
          .build();
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema, Map<String, FromJsonVisitor> visitors) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }
}
