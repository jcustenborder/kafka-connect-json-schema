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
import org.apache.kafka.connect.data.Schema;

public class FromConnectConversionKey {
  public final Schema.Type type;
  public final String schemaName;

  private FromConnectConversionKey(Schema.Type type, String schemaName) {
    this.type = type;
    this.schemaName = schemaName;
  }

  public static FromConnectConversionKey of(Schema schema) {
    return of(schema.type(), schema.name());
  }

  public static FromConnectConversionKey of(Schema.Type type) {
    return new FromConnectConversionKey(type, null);
  }

  public static FromConnectConversionKey of(Schema.Type type, String schemaName) {
    return new FromConnectConversionKey(type, schemaName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FromConnectConversionKey that = (FromConnectConversionKey) o;
    return type == that.type &&
        Objects.equal(schemaName, that.schemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, schemaName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("schemaName", schemaName)
        .toString();
  }
}
