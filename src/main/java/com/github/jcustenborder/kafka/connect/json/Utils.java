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
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Utils {

  static final ZoneId ZONE_ID = ZoneId.of("UTC");
  public static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT
      .withZone(ZONE_ID);
  public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE
      .withZone(ZONE_ID);
  public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME
      .withZone(ZONE_ID);


  public static JSONObject loadObject(Header header) {
    try (InputStream inputStream = new ByteArrayInputStream(header.value())) {
      return loadObject(inputStream);
    } catch (IOException ex) {
      throw new DataException("Could not load schema", ex);
    }
  }

  public static JSONObject loadObject(InputStream inputStream) {
    return new JSONObject(new JSONTokener(inputStream));
  }

  public static Schema loadSchema(InputStream inputStream) {
    JSONObject rawSchema = loadObject(inputStream);
    return loadSchema(rawSchema);
  }

  public static org.everit.json.schema.Schema loadSchema(JSONObject rawSchema) {
    return SchemaLoader.builder()
        .draftV7Support()
        .addFormatValidator(new DateFormatValidator())
        .addFormatValidator(new TimeFormatValidator())
        .addFormatValidator(new DateTimeFormatValidator())
        .addFormatValidator(new DecimalFormatValidator())
        .addFormatValidator(new CustomTimestampFormatValidator())
        .schemaJson(rawSchema)
        .build()
        .load()
        .build();
  }

  public static org.everit.json.schema.Schema loadSchema(Header header) {
    JSONObject rawSchema = loadObject(header);
    return loadSchema(rawSchema);
  }


  public static int scale(StringSchema schema) {
    String scale = schema.getUnprocessedProperties().get("scale").toString();
    return scale(scale);
  }

  private static int scale(String scale) {
    return Integer.parseInt(scale);
  }

  public static int scale(org.apache.kafka.connect.data.Schema connectSchema) {
    String scale = connectSchema.parameters().get(Decimal.SCALE_FIELD);
    return scale(scale);
  }

  public static JSONObject loadObject(Reader reader) {
    return new JSONObject(new JSONTokener(reader));
  }

  public static Schema loadSchema(String schemaText) {
    try (Reader reader = new StringReader(schemaText)) {
      JSONObject rawSchema = loadObject(reader);
      return loadSchema(rawSchema);
    } catch (IOException ex) {
      throw new DataException("Could not load schema", ex);
    }
  }


}
