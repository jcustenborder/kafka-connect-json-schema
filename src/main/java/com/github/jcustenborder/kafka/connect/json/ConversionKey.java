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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConversionKey {
  private static final Logger log = LoggerFactory.getLogger(ConversionKey.class);
  final Class<? extends org.everit.json.schema.Schema> schemaClass;
  final String format;
  final Boolean requiresInteger;

  private ConversionKey(Class<? extends Schema> schemaClass, String format, Boolean requiresInteger) {
    this.schemaClass = schemaClass;
    this.format = format;
    this.requiresInteger = requiresInteger;
  }

  static final String UNNAMED_FORMAT = "unnamed-format";

  public static ConversionKey of(org.everit.json.schema.Schema jsonSchema) {
    String format;
    Boolean requiresInteger;
    if (jsonSchema instanceof StringSchema) {
      StringSchema stringSchema = (StringSchema) jsonSchema;
      format = UNNAMED_FORMAT.equals(stringSchema.getFormatValidator().formatName()) ? null : stringSchema.getFormatValidator().formatName();
      requiresInteger = null;
      log.trace("jsonSchema = '{}' format = '{}'", jsonSchema, format);
    } else if (jsonSchema instanceof NumberSchema) {
      NumberSchema numberSchema = (NumberSchema) jsonSchema;
      requiresInteger = numberSchema.requiresInteger();
      format = null;
    } else {
      format = null;
      requiresInteger = null;
    }

    return new ConversionKey(jsonSchema.getClass(), format, requiresInteger);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConversionKey that = (ConversionKey) o;
    return Objects.equal(schemaClass, that.schemaClass) &&
        Objects.equal(format, that.format) &&
        Objects.equal(requiresInteger, that.requiresInteger);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schemaClass, format, requiresInteger);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("schemaClass", schemaClass)
        .add("format", format)
        .add("requiesInteger", requiresInteger)
        .toString();
  }

  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass) {
    return new ConversionKey(schemaClass, null, null);
  }

  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass, String format) {
    return new ConversionKey(schemaClass, format, null);
  }

  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass, Boolean requiesInteger) {
    return new ConversionKey(schemaClass, null, requiesInteger);
  }

}
