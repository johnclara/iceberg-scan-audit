package com.box.dataplatform.iceberg.client.avro.transformations;

import static org.junit.Assert.assertEquals;

import com.box.dataplatform.TestRecordWithMetadata;
import com.box.dseschema.cdc.credence.*;
import com.box.dseschema.common.EventMetadata;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/** */
public class EntitySchemaTransformerTest {
  // temporarily hard code schema
  @Test
  public void testInsert() throws IOException {
    EntitySchemaTransformer transformer = new EntitySchemaTransformer();

    EventMetadata eventMetadata = new EventMetadata();
    Long insertTime = 100L;
    eventMetadata.setEventTimestamp(insertTime);
    eventMetadata.setEventId(insertTime.toString() + ".insert");

    CDCMetadata cdcMetadata = new CDCMetadata();
    cdcMetadata.setEntityId(insertTime.toString());
    cdcMetadata.setEntityType("TestRecord");
    cdcMetadata.setVersionEpoch(insertTime + 1);
    cdcMetadata.setSource(Source.CREDENCE);

    EntityDiff entityDiff = new EntityDiff();
    Map<String, Object> entityState = new HashMap<String, Object>();
    entityState.put("value", 1);
    entityState.put("timestamp", insertTime);
    entityDiff.setEntityState(entityState);

    CDCEvent event = new CDCEvent();
    event.setEventMetadata(eventMetadata);
    event.setCdcMetadata(cdcMetadata);
    event.setOperation(Operation.INSERT);
    event.setDiff(entityDiff);

    Schema testRecordSchema = new TestRecordWithMetadata().getSchema();

    GenericRecord result = transformer.transform(event, testRecordSchema);
    String expectedRecord =
        "{\"dpmeta\": "
            + "{\"version\": 101, \"deleted\": 0, \"operationType\": \"INSERT\", \"eventTime\": 100, \"shardId\": null, \"requestId\": null}, "
            + "\"value\": 1, "
            + "\"timestamp\": 100"
            + "}";
    assertEquals(expectedRecord, result.toString());
  }

  @Test
  public void testUpdate() throws IOException {
    EntitySchemaTransformer transformer = new EntitySchemaTransformer();

    EventMetadata eventMetadata = new EventMetadata();
    Long insertTime = 200L;
    eventMetadata.setEventTimestamp(insertTime);
    eventMetadata.setEventId(insertTime.toString() + ".update");

    CDCMetadata cdcMetadata = new CDCMetadata();
    cdcMetadata.setEntityId(insertTime.toString());
    cdcMetadata.setEntityType("TestRecord");
    cdcMetadata.setVersionEpoch(insertTime + 1);
    cdcMetadata.setSource(Source.CREDENCE);

    EntitySnapshot entitySnapshot = new EntitySnapshot();
    Map<String, Object> preEntityState = new HashMap<String, Object>();
    preEntityState.put("value", 1);
    preEntityState.put("timestamp", 100);
    entitySnapshot.setEntityState(preEntityState);

    EntityDiff entityDiff = new EntityDiff();
    Map<String, Object> diffEntityState = new HashMap<String, Object>();
    diffEntityState.put("value", 2);
    entityDiff.setEntityState(diffEntityState);

    CDCEvent event = new CDCEvent();
    event.setEventMetadata(eventMetadata);
    event.setCdcMetadata(cdcMetadata);
    event.setOperation(Operation.UPDATE);
    event.setPre(entitySnapshot);
    event.setDiff(entityDiff);

    Schema testRecordSchema = new TestRecordWithMetadata().getSchema();

    GenericRecord result = transformer.transform(event, testRecordSchema);
    String expectedRecord =
        "{\"dpmeta\": "
            + "{\"version\": 201, \"deleted\": 0, \"operationType\": \"UPDATE\", \"eventTime\": 200, \"shardId\": null, \"requestId\": null}, "
            + "\"value\": 2, "
            + "\"timestamp\": 100"
            + "}";
    assertEquals(expectedRecord, result.toString());
  }

  @Test
  public void testDelete() throws IOException {
    EntitySchemaTransformer transformer = new EntitySchemaTransformer();

    EventMetadata eventMetadata = new EventMetadata();
    Long deleteTime = 200L;
    eventMetadata.setEventTimestamp(deleteTime);
    eventMetadata.setEventId(deleteTime.toString() + ".delete");

    CDCMetadata cdcMetadata = new CDCMetadata();
    cdcMetadata.setEntityId(deleteTime.toString());
    cdcMetadata.setEntityType("TestRecord");
    cdcMetadata.setVersionEpoch(deleteTime + 1);
    cdcMetadata.setSource(Source.CREDENCE);

    EntitySnapshot entitySnapshot = new EntitySnapshot();
    Map<String, Object> preEntityState = new HashMap<String, Object>();
    preEntityState.put("value", 2);
    preEntityState.put("timestamp", 100);
    entitySnapshot.setEntityState(preEntityState);

    CDCEvent event = new CDCEvent();
    event.setEventMetadata(eventMetadata);
    event.setCdcMetadata(cdcMetadata);
    event.setOperation(Operation.DELETE);
    event.setPre(entitySnapshot);

    Schema testRecordSchema = new TestRecordWithMetadata().getSchema();

    GenericRecord result = transformer.transform(event, testRecordSchema);
    String expectedRecord =
        "{\"dpmeta\": "
            + "{\"version\": 201, \"deleted\": 0, \"operationType\": \"DELETE\", \"eventTime\": 200, \"shardId\": null, \"requestId\": null}, "
            + "\"value\": 2, "
            + "\"timestamp\": 100"
            + "}";
    assertEquals(expectedRecord, result.toString());
  }

  @Test
  public void testPreferCanonical() throws IOException {
    EntitySchemaTransformer transformer = new EntitySchemaTransformer();

    EventMetadata eventMetadata = new EventMetadata();
    Long insertTime = 200L;
    eventMetadata.setEventTimestamp(insertTime);
    eventMetadata.setEventId(insertTime.toString() + ".update");

    CDCMetadata cdcMetadata = new CDCMetadata();
    cdcMetadata.setEntityId(insertTime.toString());
    cdcMetadata.setEntityType("TestRecord");
    cdcMetadata.setVersionEpoch(insertTime + 1);
    cdcMetadata.setSource(Source.CREDENCE);

    EntitySnapshot entitySnapshot = new EntitySnapshot();
    Map<String, Object> preEntityState = new HashMap<String, Object>();
    preEntityState.put("value", 1);
    preEntityState.put("timestamp", 100);
    entitySnapshot.setEntityState(preEntityState);

    EntityDiff entityDiff = new EntityDiff();
    Map<String, Object> diffEntityState = new HashMap<String, Object>();
    diffEntityState.put("value", 2);
    entityDiff.setEntityState(diffEntityState);

    EntitySnapshot canonicalSnapshot = new EntitySnapshot();
    Map<String, Object> canonicalEntityState = new HashMap<String, Object>();
    canonicalEntityState.put("value", 3);
    canonicalEntityState.put("timestamp", 300);
    canonicalSnapshot.setEntityState(canonicalEntityState);

    CDCEvent event = new CDCEvent();
    event.setEventMetadata(eventMetadata);
    event.setCdcMetadata(cdcMetadata);
    event.setOperation(Operation.UPDATE);
    event.setPre(entitySnapshot);
    event.setDiff(entityDiff);
    event.setCanonicalRecordInDb(canonicalSnapshot);

    Schema testRecordSchema = new TestRecordWithMetadata().getSchema();

    GenericRecord result = transformer.transform(event, testRecordSchema);
    String expectedRecord =
        "{\"dpmeta\": "
            + "{\"version\": 201, \"deleted\": 0, \"operationType\": \"UPDATE\", \"eventTime\": 200, \"shardId\": null, \"requestId\": null}, "
            + "\"value\": 3, "
            + "\"timestamp\": 300"
            + "}";
    assertEquals(expectedRecord, result.toString());
  }

  @Test
  public void testJustCanonical() throws IOException {
    EntitySchemaTransformer transformer = new EntitySchemaTransformer();

    EventMetadata eventMetadata = new EventMetadata();
    Long insertTime = 200L;
    eventMetadata.setEventTimestamp(insertTime);
    eventMetadata.setEventId(insertTime.toString() + ".update");

    CDCMetadata cdcMetadata = new CDCMetadata();
    cdcMetadata.setEntityId(insertTime.toString());
    cdcMetadata.setEntityType("TestRecord");
    cdcMetadata.setVersionEpoch(insertTime + 1);
    cdcMetadata.setSource(Source.CREDENCE);

    EntitySnapshot canonicalSnapshot = new EntitySnapshot();
    Map<String, Object> canonicalEntityState = new HashMap<String, Object>();
    canonicalEntityState.put("value", 3);
    canonicalEntityState.put("timestamp", 300);
    canonicalSnapshot.setEntityState(canonicalEntityState);

    CDCEvent event = new CDCEvent();
    event.setEventMetadata(eventMetadata);
    event.setCdcMetadata(cdcMetadata);
    event.setOperation(Operation.UPDATE);
    event.setCanonicalRecordInDb(canonicalSnapshot);

    Schema testRecordSchema = new TestRecordWithMetadata().getSchema();

    GenericRecord result = transformer.transform(event, testRecordSchema);
    String expectedRecord =
        "{\"dpmeta\": "
            + "{\"version\": 201, \"deleted\": 0, \"operationType\": \"UPDATE\", \"eventTime\": 200, \"shardId\": null, \"requestId\": null}, "
            + "\"value\": 3, "
            + "\"timestamp\": 300"
            + "}";
    assertEquals(expectedRecord, result.toString());
  }
}
