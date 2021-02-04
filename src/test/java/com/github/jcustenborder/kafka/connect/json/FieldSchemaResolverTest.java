package com.github.jcustenborder.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

public class FieldSchemaResolverTest {
  @Test
  public void validUrlForSchema() {
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "foo", "http://localhost/{schema_id}", new MockSchemaDownloader());
    URL schemaUrl = schemaResolver.urlForSchema("mySchema_v1");
    assertThat(schemaUrl.toString(), is("http://localhost/mySchema_v1"));
  }

  @Test
  public void invalidUrlForSchema() {
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "foo", "invalidUrlTemplate", new MockSchemaDownloader());
    assertThrows(ConfigException.class, () -> {
      schemaResolver.urlForSchema("mySchema_v1");
    });
  }

  @Test
  public void schemaIdInTopLevelField() throws Exception {
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "title", "http://localhost/{schema_id}", new MockSchemaDownloader());
    String schemaId = schemaResolver.schemaIdFromField(loadTestJson());
    assertThat(schemaId, is("Iztrebek"));
  }

  @Test
  public void schemaIdInNestedField() throws Exception {
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "meta.domain", "http://localhost/{schema_id}", new MockSchemaDownloader());
    String schemaId = schemaResolver.schemaIdFromField(loadTestJson());
    assertThat(schemaId, is("sl.wikipedia.org"));
  }

  @Test
  public void schemaIdFieldDoesntExist() throws Exception {
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "missing.field", "http://localhost/{schema_id}", new MockSchemaDownloader());
    assertThrows(DataException.class, () -> {
      schemaResolver.schemaIdFromField(loadTestJson());
    });
  }

  private JsonNode loadTestJson() throws IOException {
    URL resourceUrl = this.getClass().getResource("wikimedia.recentchange.data.json");
    ObjectMapper objectMapper = JacksonFactory.create();
    return objectMapper.readValue(resourceUrl, JsonNode.class);
  }

  @Test
  public void getOrLoadSchemaCaches() {
    MockSchemaDownloader schemaDownloader = new MockSchemaDownloader();
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "missing.field", "http://localhost/{schema_id}", schemaDownloader);
    Schema schema = schemaResolver.getOrLoadSchema("someSchemaId");

    assertThat(schema, is(schemaResolver.getOrLoadSchema("someSchemaId")));
    assertThat(schemaDownloader.callCount, is(1));
  }

  @Test
  public void resolveSchema() throws Exception {
    MockSchemaDownloader schemaDownloader = new MockSchemaDownloader();
    FieldSchemaResolver schemaResolver = new FieldSchemaResolver(
        "meta.domain", "http://localhost/{schema_id}", schemaDownloader);
    Schema resolvedSchema = schemaResolver.resolveSchema(null, null, loadTestJson());
    assertThat(resolvedSchema, is(schemaDownloader.schema));
    assertThat(schemaDownloader.callCount, is(1));
    assertThat(schemaDownloader.lastSchemaUrl.toString(), is("http://localhost/sl.wikipedia.org"));
  }

  private static class MockSchemaDownloader implements SchemaDownloader {
    private final Schema schema;
    int callCount;
    URL lastSchemaUrl;

    public MockSchemaDownloader() {
      this.schema = Utils.loadSchema(this.getClass().getResourceAsStream("wikimedia.recentchange.schema.json"));
    }
    @Override
    public Schema downloadSchema(URL schemaUrl) {
      callCount++;
      lastSchemaUrl = schemaUrl;
      return schema;
    }
  }
}
