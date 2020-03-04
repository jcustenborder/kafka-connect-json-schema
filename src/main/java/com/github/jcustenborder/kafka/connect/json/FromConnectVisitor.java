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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Map;

public abstract class FromConnectVisitor<T> {
  public abstract void doVisit(JsonGenerator jsonGenerator, T value) throws IOException;

  public static class StringVisitor extends FromConnectVisitor<String> {
    @Override
    public void doVisit(JsonGenerator jsonGenerator, String value) throws IOException {
      jsonGenerator.writeString(value);
    }
  }

  public static class StructVisitor extends FromConnectVisitor<Struct> {
    final Map<String, FromConnectVisitor> visitors;

    public StructVisitor(Map<String, FromConnectVisitor> visitors) {
      this.visitors = visitors;
    }

    @Override
    public void doVisit(JsonGenerator jsonGenerator, Struct value) throws IOException {
      jsonGenerator.writeStartObject();
      for (Map.Entry<String, FromConnectVisitor> e : this.visitors.entrySet()) {
        final String fieldName = e.getKey();
        final FromConnectVisitor visitor = e.getValue();
        final Object fieldValue = value.get(fieldName);
        jsonGenerator.writeFieldName(fieldName);
        visitor.doVisit(jsonGenerator, fieldValue);
      }
      jsonGenerator.writeEndObject();
    }
  }

  public static class BooleanVisitor extends FromConnectVisitor<Boolean> {
    @Override
    public void doVisit(JsonGenerator jsonGenerator, Boolean value) throws IOException {

    }
  }

  public static class BytesVisitor extends FromConnectVisitor {
    @Override
    public void doVisit(JsonGenerator jsonGenerator, Object value) throws IOException {

    }
  }

  public static class FloatVisitor extends FromConnectVisitor {
    @Override
    public void doVisit(JsonGenerator jsonGenerator, Object value) throws IOException {

    }
  }

  public static class IntegerVisitor extends FromConnectVisitor {

    @Override
    public void doVisit(JsonGenerator jsonGenerator, Object value) throws IOException {

    }
  }

}
