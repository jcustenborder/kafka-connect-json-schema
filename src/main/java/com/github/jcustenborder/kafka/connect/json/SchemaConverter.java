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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class SchemaConverter<T extends org.everit.json.schema.Schema> {
  private static final Logger log = LoggerFactory.getLogger(SchemaConverter.class);

  protected abstract SchemaBuilder schemaBuilder(T schema);

  protected abstract ConversionKey key();

  protected abstract void fromJSON(SchemaBuilder builder, T jsonSchema);

  static final Map<ConversionKey, SchemaConverter<? extends org.everit.json.schema.Schema>> LOOKUP;

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
        new ArraySchemaConverter()
    ).collect(Collectors.toMap(SchemaConverter::key, c -> c));
  }

  public static Schema fromJSON(org.everit.json.schema.Schema jsonSchema) {
    return fromJSON(jsonSchema, false);
  }

  public static Schema fromJSON(org.everit.json.schema.Schema jsonSchema, boolean isOptional) {
    if (jsonSchema instanceof ReferenceSchema) {
      ReferenceSchema referenceSchema = (ReferenceSchema) jsonSchema;
      jsonSchema = referenceSchema.getReferredSchema();
    }
    ConversionKey key = ConversionKey.of(jsonSchema);

    SchemaConverter converter = LOOKUP.get(key);

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
    converter.fromJSON(builder, jsonSchema);
    return builder.build();
  }

  static class BooleanSchemaConverter extends SchemaConverter<BooleanSchema> {
    @Override
    protected SchemaBuilder schemaBuilder(BooleanSchema schema) {
      return SchemaBuilder.bool();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(BooleanSchema.class);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, BooleanSchema jsonSchema) {

    }
  }

  static class ObjectSchemaConverter extends SchemaConverter<ObjectSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(ObjectSchema schema) {
      return SchemaBuilder.struct();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(ObjectSchema.class);
    }


    @Override
    protected void fromJSON(SchemaBuilder builder, ObjectSchema jsonSchema) {
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
            Schema connectSchema = SchemaConverter.fromJSON(propertyJsonSchema, isOptional);
            builder.field(propertyName, connectSchema);
          });
    }
  }

  static class IntegerSchemaConverter extends SchemaConverter<NumberSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(NumberSchema schema) {
      return SchemaBuilder.int64();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(NumberSchema.class, true);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, NumberSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class FloatSchemaConverter extends SchemaConverter<NumberSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(NumberSchema schema) {
      return SchemaBuilder.float64();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(NumberSchema.class, false);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, NumberSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class StringSchemaConverter extends SchemaConverter<StringSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return SchemaBuilder.string();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(StringSchema.class);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class DateSchemaConverter extends SchemaConverter<StringSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Date.builder();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(StringSchema.class, "date");
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class TimeSchemaConverter extends SchemaConverter<StringSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Time.builder();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(StringSchema.class, "time");
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class DateTimeSchemaConverter extends SchemaConverter<StringSchema> {

    @Override
    protected SchemaBuilder schemaBuilder(StringSchema schema) {
      return Timestamp.builder();
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(StringSchema.class, "date-time");
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, StringSchema jsonSchema) {
      log.trace("fromJson() - Processing '{}'", jsonSchema);
    }
  }

  static class ArraySchemaConverter extends SchemaConverter<ArraySchema> {

    @Override
    protected SchemaBuilder schemaBuilder(ArraySchema schema) {
      Schema connectSchema = SchemaConverter.fromJSON(schema.getAllItemSchema());
      return SchemaBuilder.array(connectSchema);
    }

    @Override
    protected ConversionKey key() {
      return ConversionKey.of(ArraySchema.class);
    }

    @Override
    protected void fromJSON(SchemaBuilder builder, ArraySchema jsonSchema) {

    }
  }
}
