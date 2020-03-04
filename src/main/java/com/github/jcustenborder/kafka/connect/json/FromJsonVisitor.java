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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class FromJsonVisitor<T extends JsonNode, V> {
  protected final Schema schema;

  protected FromJsonVisitor(Schema schema) {
    this.schema = schema;
  }

  public V visit(T node) {
    V result;

    if (null == node || node.isNull()) {
      result = null;
    } else {
      result = doVisit(node);
    }

    return result;
  }

  protected abstract V doVisit(T node);

  public static class StringVisitor extends FromJsonVisitor<TextNode, String> {
    public StringVisitor(Schema schema) {
      super(schema);
    }

    @Override
    public String doVisit(TextNode node) {
      return node.textValue();
    }
  }

  public static class BooleanVisitor extends FromJsonVisitor<BooleanNode, Boolean> {
    public BooleanVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Boolean doVisit(BooleanNode node) {
      return node.booleanValue();
    }
  }

  public static class StructVisitor extends FromJsonVisitor<ObjectNode, Struct> {
    private final Map<String, FromJsonVisitor> visitors;

    public StructVisitor(Schema schema, Map<String, FromJsonVisitor> visitors) {
      super(schema);
      this.visitors = visitors;
    }

    @Override
    protected Struct doVisit(ObjectNode node) {
      Struct result = new Struct(this.schema);
      visitors.forEach((fieldName, visitor) -> {
        JsonNode rawValue = node.get(fieldName);
        Object convertedValue = visitor.visit(rawValue);
        result.put(fieldName, convertedValue);
      });

      return result;
    }
  }

  public static class IntegerVisitor extends FromJsonVisitor<NumericNode, Number> {
    public IntegerVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Number doVisit(NumericNode node) {
      return null;
    }
  }

  public static class FloatVisitor extends FromJsonVisitor<NumericNode, Number> {
    public FloatVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Number doVisit(NumericNode node) {
      return null;
    }
  }

  public static class DateVisitor extends FromJsonVisitor<TextNode, java.util.Date> {
    public DateVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Date doVisit(TextNode node) {
      return null;
    }
  }

  public static class TimeVisitor extends FromJsonVisitor<TextNode, java.util.Date> {
    public TimeVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Date doVisit(TextNode node) {
      return null;
    }
  }

  public static class DateTimeVisitor extends FromJsonVisitor<TextNode, java.util.Date> {
    public DateTimeVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected Date doVisit(TextNode node) {
      return null;
    }
  }

  public static class ArrayVisitor extends FromJsonVisitor<ArrayNode, List> {
    public ArrayVisitor(Schema schema) {
      super(schema);
    }

    @Override
    protected List doVisit(ArrayNode node) {
      return null;
    }
  }

}
