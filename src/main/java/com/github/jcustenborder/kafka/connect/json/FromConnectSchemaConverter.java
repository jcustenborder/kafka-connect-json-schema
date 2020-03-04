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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FromConnectSchemaConverter {
  private static final Logger log = LoggerFactory.getLogger(FromConnectSchemaConverter.class);
  static final Map<Type, Map<String, String>> PRIMITIVE_TYPES;

  static {
    Map<Type, Map<String, String>> primitiveTypes = new LinkedHashMap<>();
    primitiveTypes.put(Type.BOOLEAN, ImmutableMap.of("type", "boolean"));
    primitiveTypes.put(Type.BYTES, ImmutableMap.of("type", "string", "contentEncoding", "base64"));
    primitiveTypes.put(Type.FLOAT32, ImmutableMap.of("type", "number"));
    primitiveTypes.put(Type.FLOAT64, ImmutableMap.of("type", "number"));
    primitiveTypes.put(Type.INT8, ImmutableMap.of("type", "integer"));
    primitiveTypes.put(Type.INT16, ImmutableMap.of("type", "integer"));
    primitiveTypes.put(Type.INT32, ImmutableMap.of("type", "integer"));
    primitiveTypes.put(Type.INT64, ImmutableMap.of("type", "integer"));
    primitiveTypes.put(Type.STRING, ImmutableMap.of("type", "string"));
    PRIMITIVE_TYPES = ImmutableMap.copyOf(primitiveTypes);
  }

  static final Map<Type, FromConnectVisitor> PRIMITIVE_VISITORS;

  static {
    Map<Type, FromConnectVisitor> primitiveVisitors = new LinkedHashMap<>();
    primitiveVisitors.put(Type.BOOLEAN, new FromConnectVisitor.BooleanVisitor());
    primitiveVisitors.put(Type.BYTES, new FromConnectVisitor.BytesVisitor());
    primitiveVisitors.put(Type.FLOAT32, new FromConnectVisitor.FloatVisitor());
    primitiveVisitors.put(Type.FLOAT64, new FromConnectVisitor.FloatVisitor());
    primitiveVisitors.put(Type.INT8, new FromConnectVisitor.IntegerVisitor());
    primitiveVisitors.put(Type.INT16, new FromConnectVisitor.IntegerVisitor());
    primitiveVisitors.put(Type.INT32, new FromConnectVisitor.IntegerVisitor());
    primitiveVisitors.put(Type.INT64, new FromConnectVisitor.IntegerVisitor());
    primitiveVisitors.put(Type.STRING, new FromConnectVisitor.StringVisitor());
    PRIMITIVE_VISITORS = ImmutableMap.copyOf(primitiveVisitors);
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
    if (!Strings.isNullOrEmpty(schema.name())) {
      result.put("title", schema.name());
    }
    log.trace("toJsonSchema() - Checking for primitive '{}'", schema.type());
    Map<String, String> primitiveType = PRIMITIVE_TYPES.get(schema.type());
    if (null != primitiveType) {
      primitiveType.forEach(result::put);
      FromConnectVisitor visitor = PRIMITIVE_VISITORS.get(schema.type());
      visitors.add(visitor);
      return result;
    }

    if (Type.ARRAY == schema.type()) {
      result.put("type", "array");

      if (Type.STRUCT == schema.valueSchema().type()) {
        Definition definition = definitions.computeIfAbsent(schema.valueSchema(), s -> {
          List<FromConnectVisitor> childVisitors = new ArrayList<>();
          JSONObject fieldJsonSchema = toJsonSchema(schema.valueSchema(), definitions, childVisitors);
          String definitionName = schema.valueSchema().name().toLowerCase();
          return Definition.of(fieldJsonSchema, definitionName, childVisitors);
        });
        result.put("items", definition.ref());
      } else {
        JSONObject arrayValueSchema = toJsonSchema(schema.valueSchema(), definitions, visitors);
        result.put("items", arrayValueSchema);
      }
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


}
