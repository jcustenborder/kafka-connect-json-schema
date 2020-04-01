package com.github.jcustenborder.kafka.connect.json;

import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FromJsonTest {
  private static final Logger log = LoggerFactory.getLogger(FromJsonTest.class);
  FromJson<SinkRecord> transform;

  @BeforeEach
  public void beforeEach() {
    this.transform = new FromJson.Value<>();
  }

  @Test
  public void basic() throws IOException {
    byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream(
        "basic.data.json"
    ));
    File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/basic.schema.json");
    Map<String, String> settings = ImmutableMap.of(
        FromJsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString()
    );
    this.transform.configure(settings);
    SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
    SinkRecord transformedRecord = this.transform.apply(inputRecord);
    assertNotNull(transformedRecord);
    assertNotNull(transformedRecord.value());
    assertTrue(transformedRecord.value() instanceof Struct);
    Struct actual = (Struct) transformedRecord.value();
    log.info("actual = '{}'", actual);
  }

  @Test
  public void validate() throws IOException {
    byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream(
        "basic.data.json"
    ));
    File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/geo.schema.json");
    Map<String, String> settings = ImmutableMap.of(
        FromJsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString(),
        FromJsonConfig.VALIDATE_JSON_ENABLED_CONF, "true"
    );
    this.transform.configure(settings);
    SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
    DataException exception = assertThrows(DataException.class, ()->{
      SinkRecord transformedRecord = this.transform.apply(inputRecord);
    });

    assertTrue(exception.getMessage().contains("required key [latitude] not found"));
    assertTrue(exception.getMessage().contains("required key [longitude] not found"));
  }

}
