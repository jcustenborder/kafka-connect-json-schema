package com.github.jcustenborder.kafka.connect.json;

import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FromJsonTest {
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
        FromJsonConfig.SCHEMA_LOCATION_CONF, schemaFile.toURI().toString()
    );
    this.transform.configure(settings);
    SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
    SinkRecord actual = this.transform.apply(inputRecord);
    assertNotNull(actual);
  }



}
