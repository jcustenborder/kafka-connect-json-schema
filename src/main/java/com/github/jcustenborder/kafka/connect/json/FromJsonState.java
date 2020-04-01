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


class FromJsonState {
  public final org.everit.json.schema.Schema jsonSchema;
  public final org.apache.kafka.connect.data.Schema schema;
  public final FromJsonVisitor visitor;

  FromJsonState(org.everit.json.schema.Schema jsonSchema, org.apache.kafka.connect.data.Schema schema, FromJsonVisitor visitor) {
    this.jsonSchema = jsonSchema;
    this.schema = schema;
    this.visitor = visitor;
  }

  public static FromJsonState of(org.everit.json.schema.Schema jsonSchema, org.apache.kafka.connect.data.Schema schema, FromJsonVisitor visitor) {
    return new FromJsonState(jsonSchema, schema, visitor);
  }
}
