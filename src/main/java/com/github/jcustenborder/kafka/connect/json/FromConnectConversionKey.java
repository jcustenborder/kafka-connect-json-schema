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
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

public class FromConnectConversionKey {
  public final Schema.Type type;
  public final String schemaName;
  public final Integer scale;

  private FromConnectConversionKey(Schema.Type type, String schemaName, Integer scale) {
    this.type = type;
    this.schemaName = schemaName;
    this.scale = scale;
  }

  public static FromConnectConversionKey of(Schema schema) {
    Integer scale;
    if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      String scaleText = schema.parameters().get(Decimal.SCALE_FIELD);
      scale = Integer.parseInt(scaleText);
    } else {
      scale = null;
    }
    return of(schema.type(), schema.name(), scale);
  }

  public static FromConnectConversionKey of(Schema.Type type) {
    return new FromConnectConversionKey(type, null, null);
  }

  public static FromConnectConversionKey of(Schema.Type type, String schemaName) {
    return new FromConnectConversionKey(type, schemaName, null);
  }

  public static FromConnectConversionKey of(Schema.Type type, String schemaName, Integer scale) {
    return new FromConnectConversionKey(type, schemaName, scale);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FromConnectConversionKey that = (FromConnectConversionKey) o;
    return type == that.type &&
        Objects.equal(schemaName, that.schemaName) &&
        Objects.equal(scale, that.scale);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, schemaName, scale);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("type", type)
        .add("schemaName", schemaName)
        .add("scale", scale)
        .toString();
  }
}
