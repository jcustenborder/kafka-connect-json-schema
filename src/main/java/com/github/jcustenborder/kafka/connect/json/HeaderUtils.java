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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HeaderUtils {

  public static JSONObject loadObject(Header header) {
    try (InputStream inputStream = new ByteArrayInputStream(header.value())) {
      return new JSONObject(new JSONTokener(inputStream));
    } catch (IOException ex) {
      throw new DataException("Could not load schema", ex);
    }
  }

  public static org.everit.json.schema.Schema loadSchema(Header header) {
    JSONObject rawSchema = loadObject(header);
    return SchemaLoader.builder()
        .draftV7Support()
        .addFormatValidator(new DateFormatValidator())
        .addFormatValidator(new TimeFormatValidator())
        .addFormatValidator(new DateTimeFormatValidator())
        .schemaJson(rawSchema)
        .build()
        .load()
        .build();
  }
}
