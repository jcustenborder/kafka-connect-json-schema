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
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FromJsonSchemaConverterFactory {
  private static final Logger log = LoggerFactory.getLogger(FromJsonSchemaConverterFactory.class);
  private final Map<
      FromJsonConversionKey,
      FromJsonSchemaConverter<? extends Schema, ? extends JsonNode, ?>
      > lookup;
  private final JsonConfig config;
  private final FromJsonConversionKey genericStringKey;

  public FromJsonSchemaConverterFactory(JsonConfig config) {
    this.config = config;
    FromJsonSchemaConverter.StringSchemaConverter stringSchemaConverter = new FromJsonSchemaConverter.StringSchemaConverter(this, config);
    genericStringKey = stringSchemaConverter.key();
    lookup = Stream.of(
        stringSchemaConverter,
        new FromJsonSchemaConverter.ObjectSchemaConverter(this, config),
        new FromJsonSchemaConverter.IntegerSchemaConverter(this, config),
        new FromJsonSchemaConverter.BooleanSchemaConverter(this, config),
        new FromJsonSchemaConverter.TimeSchemaConverter(this, config),
        new FromJsonSchemaConverter.DateSchemaConverter(this, config),
        new FromJsonSchemaConverter.DateTimeSchemaConverter(this, config),
        new FromJsonSchemaConverter.FloatSchemaConverter(this, config),
        new FromJsonSchemaConverter.ArraySchemaConverter(this, config),
        new FromJsonSchemaConverter.BytesSchemaConverter(this, config),
        new FromJsonSchemaConverter.DecimalSchemaConverter(this, config),
        new FromJsonSchemaConverter.CustomTimestampConverter(this, config)
    ).collect(Collectors.toMap(FromJsonSchemaConverter::key, c -> c));
  }

  public FromJsonState fromJSON(org.everit.json.schema.Schema jsonSchema) {
    return fromJSON(jsonSchema, false);
  }

  public FromJsonState fromJSON(org.everit.json.schema.Schema jsonSchema, boolean isOptional) {
    if (jsonSchema instanceof ReferenceSchema) {
      ReferenceSchema referenceSchema = (ReferenceSchema) jsonSchema;
      jsonSchema = referenceSchema.getReferredSchema();
    } else if (jsonSchema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) jsonSchema;
      List<Schema> nonNullSubSchemas = combinedSchema
          .getSubschemas()
          .stream()
          .filter(s -> !(s instanceof NullSchema))
          .collect(Collectors.toList());
      if (1 != nonNullSubSchemas.size()) {
        throw new UnsupportedOperationException(
            String.format(
                "More than one choice for non null schemas. Schema location %s: %s",
                jsonSchema.getSchemaLocation(),
                Joiner.on(", ").join(nonNullSubSchemas)
            )
        );
      }
      jsonSchema = nonNullSubSchemas.get(0);
    }
    FromJsonConversionKey key = FromJsonConversionKey.of(jsonSchema);

    FromJsonSchemaConverter converter = lookup.get(key);

    if (null == converter && jsonSchema instanceof StringSchema) {
      log.trace("fromJSON() - falling back to string passthrough for {}", jsonSchema);
      converter = lookup.get(genericStringKey);
    }

    if (null == converter) {
      throw new UnsupportedOperationException(
          String.format("Schema type is not supported. %s:%s", jsonSchema.getClass().getName(), jsonSchema)
      );
    }

    SchemaBuilder builder = converter.schemaBuilder(jsonSchema);
    if (!Strings.isNullOrEmpty(jsonSchema.getTitle())) {
      builder.name(cleanName(jsonSchema.getTitle()));
    }
    if (!Strings.isNullOrEmpty(jsonSchema.getDescription())) {
      builder.doc(jsonSchema.getDescription());
    }
    if (isOptional) {
      builder.optional();
    }
    Map<String, FromJsonVisitor> visitors = new LinkedHashMap<>();
    converter.fromJSON(builder, jsonSchema, visitors);
    org.apache.kafka.connect.data.Schema schema = builder.build();
    FromJsonVisitor visitor = converter.jsonVisitor(schema, visitors);
    return FromJsonState.of(jsonSchema, schema, visitor);
  }

  private String cleanName(String title) {
    String result = title.replaceAll("[\\/]+", ".");
    return result;
  }

}
