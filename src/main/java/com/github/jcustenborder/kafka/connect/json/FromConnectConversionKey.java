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

  public static FromConnectConversionKey of(Schema schema) {
    return of(schema.type(), schema.name());
  }

  public static FromConnectConversionKey of(Schema.Type type) {
    return new FromConnectConversionKey(type, null);
  }
  public static FromConnectConversionKey of(Schema.Type type, String schemaName) {
    return new FromConnectConversionKey(type, schemaName);
  }
}
