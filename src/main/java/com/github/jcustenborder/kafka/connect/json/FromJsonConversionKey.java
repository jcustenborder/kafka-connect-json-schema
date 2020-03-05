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

class FromJsonConversionKey {
  static final String UNNAMED_FORMAT = "unnamed-format";
  private static final Logger log = LoggerFactory.getLogger(FromJsonConversionKey.class);
  final Class<? extends org.everit.json.schema.Schema> schemaClass;
  final String format;
  final Boolean requiresInteger;
  final String contentEncoding;

  private FromJsonConversionKey(Class<? extends Schema> schemaClass, String format, Boolean requiresInteger, String contentEncoding) {
    this.schemaClass = schemaClass;
    this.format = format;
    this.requiresInteger = requiresInteger;
    this.contentEncoding = contentEncoding;
  }

  public static FromJsonConversionKey of(org.everit.json.schema.Schema jsonSchema) {
    String format;
    Boolean requiresInteger;
    String contentEncoding;
    if (jsonSchema instanceof StringSchema) {
      StringSchema stringSchema = (StringSchema) jsonSchema;
      format = UNNAMED_FORMAT.equals(stringSchema.getFormatValidator().formatName()) ? null : stringSchema.getFormatValidator().formatName();
      contentEncoding = (String) stringSchema.getUnprocessedProperties().get("contentEncoding");
      requiresInteger = null;
      log.trace("jsonSchema = '{}' format = '{}'", jsonSchema, format);
    } else if (jsonSchema instanceof NumberSchema) {
      NumberSchema numberSchema = (NumberSchema) jsonSchema;
      requiresInteger = numberSchema.requiresInteger();
      format = null;
      contentEncoding = null;
    } else {
      format = null;
      requiresInteger = null;
      contentEncoding = null;
    }

    return new FromJsonConversionKey(jsonSchema.getClass(), format, requiresInteger, contentEncoding);
  }

  public static Builder from(Class<? extends org.everit.json.schema.Schema> schemaClass) {
    return new Builder().schemaClass(schemaClass);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FromJsonConversionKey that = (FromJsonConversionKey) o;
    return Objects.equal(schemaClass, that.schemaClass) &&
        Objects.equal(format, that.format) &&
        Objects.equal(requiresInteger, that.requiresInteger) &&
        Objects.equal(contentEncoding, that.contentEncoding);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schemaClass, format, requiresInteger, contentEncoding);
  }

  //  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass) {
//    return new ConversionKey(schemaClass, null, null, contentMediaType);
//  }
//
//  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass, String format) {
//    return new ConversionKey(schemaClass, format, null, contentMediaType);
//  }
//
//  public static ConversionKey of(Class<? extends org.everit.json.schema.Schema> schemaClass, Boolean requiesInteger) {
//    return new ConversionKey(schemaClass, null, requiesInteger, contentMediaType);
//  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("schemaClass", schemaClass)
        .add("format", format)
        .add("requiresInteger", requiresInteger)
        .add("contentEncoding", contentEncoding)
        .toString();
  }

  static class Builder {
    Class<? extends org.everit.json.schema.Schema> schemaClass;
    String format;
    Boolean requiresInteger;
    String contentEncoding;
    private Builder() {
    }

    public Builder schemaClass(Class<? extends Schema> schemaClass) {
      this.schemaClass = schemaClass;
      return this;
    }

    public Builder format(String format) {
      this.format = format;
      return this;
    }

    public Builder requiresInteger(Boolean requiresInteger) {
      this.requiresInteger = requiresInteger;
      return this;
    }

    public Builder contentEncoding(String contentMediaType) {
      this.contentEncoding = contentMediaType;
      return this;
    }

    public FromJsonConversionKey build() {
      return new FromJsonConversionKey(this.schemaClass, this.format, this.requiresInteger, this.contentEncoding);
    }
  }

}
