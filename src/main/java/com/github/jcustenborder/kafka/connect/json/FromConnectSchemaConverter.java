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
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FromConnectSchemaConverter {
  static final Map<FromConnectConversionKey, Map<String, String>> PRIMITIVE_TYPES;
  static final Map<FromConnectConversionKey, FromConnectVisitor> PRIMITIVE_VISITORS;
  private static final Logger log = LoggerFactory.getLogger(FromConnectSchemaConverter.class);

  private static void addType(
      Map<FromConnectConversionKey, FromConnectVisitor> primitiveVisitors,
      Map<FromConnectConversionKey, Map<String, String>> primitiveTypes,
      FromConnectConversionKey key,
      FromConnectVisitor visitor,
      ImmutableMap<String, String> properties) {

    primitiveTypes.put(key, properties);
    primitiveVisitors.put(key, visitor);
  }

  static {
    Map<FromConnectConversionKey, FromConnectVisitor> visitors = new LinkedHashMap<>();
    Map<FromConnectConversionKey, Map<String, String>> types = new LinkedHashMap<>();
    addType(
        visitors, types,
        FromConnectConversionKey.of(Type.BOOLEAN),
        new FromConnectVisitor.BooleanVisitor(),
        ImmutableMap.of("type", "boolean")
    );
    addType(
        visitors, types,
        FromConnectConversionKey.of(Type.BYTES),
        new FromConnectVisitor.BytesVisitor(),
        ImmutableMap.of("type", "string", "contentEncoding", "base64")
    );
    addType(
        visitors, types,
        FromConnectConversionKey.of(Type.FLOAT32),
        new FromConnectVisitor.FloatVisitor(),
        ImmutableMap.of("type", "number")
    );
    addType(
        visitors, types,
        FromConnectConversionKey.of(Type.FLOAT64),
        new FromConnectVisitor.FloatVisitor(),
        ImmutableMap.of("type", "number")
    );
    Stream.of(Type.INT8, Type.INT16, Type.INT32, Type.INT64)
        .forEach(type -> {
          addType(
              visitors, types,
              FromConnectConversionKey.of(type),
              new FromConnectVisitor.IntegerVisitor(),
              ImmutableMap.of("type", "integer")
          );
        });
    IntStream.range(0, 100)
        .forEach(scale -> {
          Schema decimalSchema = Decimal.schema(scale);
          addType(
              visitors, types,
              FromConnectConversionKey.of(decimalSchema),
              new FromConnectVisitor.DecimalVisitor(scale),
              ImmutableMap.of(
                  "type", "string",
                  "format", "decimal",
                  "scale", Integer.toString(scale)
              )
          );
        });

    visitors.put(FromConnectConversionKey.of(Type.STRING), new FromConnectVisitor.StringVisitor());
    types.put(FromConnectConversionKey.of(Type.STRING), ImmutableMap.of("type", "string"));

    visitors.put(FromConnectConversionKey.of(Date.SCHEMA), new FromConnectVisitor.DateVisitor());
    types.put(FromConnectConversionKey.of(Date.SCHEMA), ImmutableMap.of("type", "string", "format", "date"));

    visitors.put(FromConnectConversionKey.of(Time.SCHEMA), new FromConnectVisitor.TimeVisitor());
    types.put(FromConnectConversionKey.of(Time.SCHEMA), ImmutableMap.of("type", "string", "format", "time"));

    visitors.put(FromConnectConversionKey.of(Timestamp.SCHEMA), new FromConnectVisitor.DateTimeVisitor());
    types.put(FromConnectConversionKey.of(Timestamp.SCHEMA), ImmutableMap.of("type", "string", "format", "date-time"));
    PRIMITIVE_TYPES = ImmutableMap.copyOf(types);
    PRIMITIVE_VISITORS = ImmutableMap.copyOf(visitors);
  }

  public static FromConnectState toJsonSchema(org.apache.kafka.connect.data.Schema schema, String headerName) {
    Map<Schema, Definition> definitions = new LinkedHashMap<>();
    List<FromConnectVisitor> visitors = new ArrayList<>();
    JSONObject result = toJsonSchema(schema, definitions, visitors);
    result.put("$schema", "http://json-schema.org/draft-07/schema#");
    if (!definitions.isEmpty()) {
      //definitions
      JSONObject definitionsObject = new JSONObject();
      definitions.forEach((definitionName, definition) -> {
        definitionsObject.put(definition.name(), definition.jsonSchema());
      });
      result.put("definitions", definitionsObject);
    }

    Header header = new RecordHeader(
        headerName,
        result.toString().getBytes(Charsets.UTF_8)
    );

    FromConnectVisitor visitor = visitors.get(0);
    return FromConnectState.of(header, visitor);
  }

  private static JSONObject toJsonSchema(org.apache.kafka.connect.data.Schema schema, Map<Schema, Definition> definitions, List<FromConnectVisitor> visitors) {
    JSONObject result = new JSONObject();
    if (!Strings.isNullOrEmpty(schema.doc())) {
      result.put("description", schema.doc());
    }
    FromConnectConversionKey key = FromConnectConversionKey.of(schema);
    log.trace("toJsonSchema() - Checking for '{}'", key);
    Map<String, String> primitiveType = PRIMITIVE_TYPES.get(key);
    if (null != primitiveType) {
      primitiveType.forEach(result::put);
      FromConnectVisitor visitor = PRIMITIVE_VISITORS.get(key);
      visitors.add(visitor);
      return result;
    }

    if (!Strings.isNullOrEmpty(schema.name())) {
      result.put("title", schema.name());
    }


    if (Type.ARRAY == schema.type()) {
      result.put("type", "array");
      FromConnectVisitor elementVisitor;
      if (Type.STRUCT == schema.valueSchema().type()) {
        Definition definition = definitions.computeIfAbsent(schema.valueSchema(), s -> {
          List<FromConnectVisitor> childVisitors = new ArrayList<>();
          JSONObject fieldJsonSchema = toJsonSchema(schema.valueSchema(), definitions, childVisitors);
          String definitionName = schema.valueSchema().name().toLowerCase();
          return Definition.of(fieldJsonSchema, definitionName, childVisitors);
        });
        result.put("items", definition.ref());
        elementVisitor = definition.visitors().get(0);
      } else {
        List<FromConnectVisitor> childVisitors = new ArrayList<>();
        JSONObject arrayValueSchema = toJsonSchema(schema.valueSchema(), definitions, childVisitors);
        elementVisitor = childVisitors.get(0);
        result.put("items", arrayValueSchema);
      }
      visitors.add(new FromConnectVisitor.ArrayVisitor(elementVisitor));
    }
    if (Type.STRUCT == schema.type()) {
      List<String> requiredFields = new ArrayList<>(schema.fields().size());
      Map<String, JSONObject> properties = new LinkedHashMap<>(schema.fields().size());
      Map<String, FromConnectVisitor> structVisitors = new LinkedHashMap<>();
      schema.fields().forEach(field -> {
        log.trace("toJsonSchema() - field:{} type:{}", field.name(), field.schema().type());
        List<FromConnectVisitor> childVisitors = new ArrayList<>();
        if (!field.schema().isOptional()) {
          requiredFields.add(field.name());
        }
        if (Type.STRUCT == field.schema().type()) {
          Definition definition = definitions.computeIfAbsent(field.schema(), s -> {
            List<FromConnectVisitor> definitionVisitors = new ArrayList<>();
            JSONObject fieldJsonSchema = toJsonSchema(field.schema(), definitions, definitionVisitors);
            String definitionName = field.schema().name().toLowerCase();
            return Definition.of(fieldJsonSchema, definitionName, definitionVisitors);
          });
          childVisitors.addAll(definition.visitors());
          properties.put(field.name(), definition.ref());
        } else {
          JSONObject fieldJsonSchema = toJsonSchema(field.schema(), definitions, childVisitors);
          properties.put(field.name(), fieldJsonSchema);
        }
        FromConnectVisitor fieldVisitor = childVisitors.get(0);
        structVisitors.put(field.name(), fieldVisitor);
      });
      result.put("properties", properties);
      result.put("required", requiredFields);
      result.put("type", "object");
      visitors.add(new FromConnectVisitor.StructVisitor(structVisitors));
    }


    log.trace("toJsonSchema() - '{}' is not primitive.", schema.type());

    return result;
  }

  static class Definition {
    private final JSONObject jsonSchema;
    private final String name;
    private final List<FromConnectVisitor> visitors;


    private Definition(JSONObject jsonSchema, String name, List<FromConnectVisitor> visitors) {
      this.jsonSchema = jsonSchema;
      this.name = name;
      this.visitors = visitors;
    }

    public static Definition of(JSONObject jsonSchema, String ref, List<FromConnectVisitor> visitors) {
      return new Definition(jsonSchema, ref, visitors);
    }

    public JSONObject jsonSchema() {
      return this.jsonSchema;
    }

    public String name() {
      return this.name;
    }

    public JSONObject ref() {
      return new JSONObject()
          .put("$ref", String.format("#/definitions/%s", this.name));
    }

    public List<FromConnectVisitor> visitors() {
      return this.visitors;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("jsonSchema", jsonSchema)
          .add("name", name)
          .toString();
    }
  }


}
