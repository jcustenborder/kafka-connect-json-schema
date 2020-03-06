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
import com.google.common.base.Strings;
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
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class FromJsonSchemaConverter<T extends org.everit.json.schema.Schema, J extends JsonNode, V> {
  static final Map<
      FromJsonConversionKey,
      FromJsonSchemaConverter<? extends org.everit.json.schema.Schema, ? extends JsonNode, ?>
      > LOOKUP;
  private static final Logger log = LoggerFactory.getLogger(FromJsonSchemaConverter.class);

  static {
    LOOKUP = Stream.of(
        new ObjectSchemaConverter(),
        new IntegerSchemaConverter(),
        new StringSchemaConverter(),
        new BooleanSchemaConverter(),
        new TimeSchemaConverter(),
        new DateSchemaConverter(),
        new DateTimeSchemaConverter(),
        new FloatSchemaConverter(),
        new ArraySchemaConverter(),
        new BytesSchemaConverter(),
        new DecimalSchemaConverter()
    ).collect(Collectors.toMap(FromJsonSchemaConverter::key, c -> c));
  }

  public static FromJsonState fromJSON(org.everit.json.schema.Schema jsonSchema) {
    return fromJSON(jsonSchema, false);
  }

  public static FromJsonState fromJSON(org.everit.json.schema.Schema jsonSchema, boolean isOptional) {
    if (jsonSchema instanceof ReferenceSchema) {
      ReferenceSchema referenceSchema = (ReferenceSchema) jsonSchema;
      jsonSchema = referenceSchema.getReferredSchema();
    }
    FromJsonConversionKey key = FromJsonConversionKey.of(jsonSchema);

    FromJsonSchemaConverter converter = LOOKUP.get(key);

    if (null == converter) {
      throw new UnsupportedOperationException(
          String.format("Schema type is not supported. %s:%s", jsonSchema.getClass().getName(), jsonSchema)
      );
    }

    SchemaBuilder builder = converter.schemaBuilder(jsonSchema);
    if (!Strings.isNullOrEmpty(jsonSchema.getTitle())) {
      builder.name(jsonSchema.getTitle());
    }
    if (!Strings.isNullOrEmpty(jsonSchema.getDescription())) {
      builder.doc(jsonSchema.getDescription());
    }
    if (isOptional) {
      builder.optional();
    }
    Map<String, FromJsonVisitor> visitors = new LinkedHashMap<>();
    converter.fromJSON(builder, jsonSchema, visitors);
    Schema schema = builder.build();
    FromJsonVisitor visitor = converter.jsonVisitor(schema, visitors);
    return FromJsonState.of(schema, visitor);
  }

  protected abstract SchemaBuilder schemaBuilder(T schema);

  protected abstract FromJsonConversionKey key();

  protected abstract FromJsonVisitor<J, V> jsonVisitor(Schema connectSchema, Map<String, FromJsonVisitor> visitors);

  protected abstract void fromJSON(SchemaBuilder builder, T jsonSchema, Map<String, FromJsonVisitor> visitors);

  static class BooleanSchemaConverter extends FromJsonSchemaConverter<BooleanSchema, BooleanNode, Boolean> {
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
          .sorted(Map.Entry.comparingByKey())
          .forEach(e -> {
            final String propertyName = e.getKey();
            final org.everit.json.schema.Schema propertyJsonSchema = e.getValue();
            final boolean isOptional = !requiredProperties.contains(propertyName);
            log.trace("fromJson() - Processing property '{}' '{}'", propertyName, propertyJsonSchema);
            FromJsonState state = FromJsonSchemaConverter.fromJSON(propertyJsonSchema, isOptional);
            builder.field(propertyName, state.schema);
            visitors.put(propertyName, state.visitor);
          });
    }
  }

  static class IntegerSchemaConverter extends FromJsonSchemaConverter<NumberSchema, NumericNode, Number> {

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
    public DecimalSchemaConverter() {

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

    @Override
    protected SchemaBuilder schemaBuilder(ArraySchema schema) {
      FromJsonState state = FromJsonSchemaConverter.fromJSON(schema.getAllItemSchema());
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
      FromJsonState state = FromJsonSchemaConverter.fromJSON(jsonSchema.getAllItemSchema());
      visitors.put("item", state.visitor);
    }
  }
}
