package com.box.dataplatform.iceberg.client.avro.transformations;

import com.box.dataplatform.iceberg.client.avro.GenericRecordTransformer;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class which holds transformation functions from CDCEvent to Entity Records
 *
 * <p>CDCEvent Schema <a
 * href="https://git.dev.box.net/messaging/schema-definitions/tree/master/packages/cdc-credence">
 * cdc-credence</a>
 *
 * <p>Entity Schemas <a
 * href="https://git.dev.box.net/DataPlatform/gbox-schemas/tree/master/gbox/src/main/avro">
 * gbox-schemas</a>
 */
public class EntitySchemaTransformer extends GenericRecordTransformer {
  protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

  /**
   * Fields in Entity Objects. Enforced by: <a
   * href="https://git.dev.box.net/DataPlatform/gbox-schemas/blob/master/gbox-schemas/src/main/scala/com/box/dataplatform/avro/GboxAvroGenerator.scala">
   * GboxAvroGenerator</a>
   */
  private static final String DP_METADATA_FIELD_NAME = "dpmeta";

  /**
   * Fields in DpMetadata object: See <a
   * href="https://git.dev.box.net/DataPlatform/gbox-schemas/blob/master/DPMetadata.avsc">DPMetadata</a>
   */
  private static final String DP_META_FIELD_NAME = "dpmeta";

  private static final String VERSION_FIELD_NAME = "version";
  private static final String DELETED_FIELD_NAME = "deleted";
  private static final String DP_OPERATION_FIELD_NAME = "operationType";
  private static final String EVENT_TIME_FIELD_NAME = "eventTime";
  private static final String SHARD_ID_FIELD_NAME = "shardId";
  private static final String REQUEST_ID_FIELD_NAME = "requestId";

  private static final String EVENT_METADATA_FIELD_NAME = "event_metadata"; // holds EventMetadata
  private static final String EVENT_TIMESTAMP_FIELD_NAME = "event_timestamp"; // holds long

  /**
   * Fields in CDCEvent object: See <a
   * href="https://git.dev.box.net/messaging/schema-definitions/blob/master/packages/cdc-credence/CDCEvent.avsc">
   * CDCEvent</a>
   */
  private static final String DIFF_FIELD_NAME = "diff"; // holds EntitySnapshot

  private static final String PRE_FIELD_NAME = "pre"; // holds EntitySnapshot
  private static final String CANONICAL_FIELD_NAME =
      "canonical_record_in_db"; // holds EntitySnapshot

  private static final String CDC_METADATA_FIELD_NAME = "cdc_metadata"; // holds CdcMetadata
  private static final String CDC_OPERATION_FIELD_NAME = "operation"; // holds operation enum
  private static final String CDC_SHARD_ID_FIELD_NAME = "shard_id"; // holds optional int

  private static final String ACTOR_METADATA_FIELD_NAME = "actor"; // holds optoinal ActorMetadata
  private static final String CDC_REQUEST_ID_FIELD_NAME = "request_id"; // holds optional string

  /**
   * Fields in CDCMetadata object: See <a
   * href="https://git.dev.box.net/messaging/schema-definitions/blob/master/packages/cdc-credence/CDCMetadata.avsc">
   * CDCEvent</a>
   */
  private static final String VERSION_EPOCH_FIELD_NAME = "version_epoch";

  /**
   * Fields in EntitySnapshot and EntityDiff object: See <a
   * href="https://git.dev.box.net/messaging/schema-definitions/blob/master/packages/cdc-credence/EntitySnapshot.avsc">
   * EntitySnapshot</a> See <a
   * href="https://git.dev.box.net/messaging/schema-definitions/blob/master/packages/cdc-credence/EntityDiff.avsc">
   * EntityDiff</a>
   */
  private static final String ENTITY_STATE_FIELD_NAME = "entity_state";

  public Boolean hasCanonicalField(GenericRecord record) {
    return record.get(CANONICAL_FIELD_NAME) != null;
  }

  /** Returns a comma delimited string of the provided field names for logging */
  private String getEntitySnapshotFieldNames(GenericRecord record, String entitySnapshotFieldName) {
    Map<Object, Object> entityState = getEntityState(record, entitySnapshotFieldName);
    StringBuilder fields = new StringBuilder();
    for (Object key : entityState.keySet()) {
      fields.append(key).append(",");
    }
    return fields.toString();
  }

  public String getPreFieldNames(GenericRecord record) {
    return getEntitySnapshotFieldNames(record, PRE_FIELD_NAME);
  }

  public String getDiffFieldNames(GenericRecord record) {
    return getEntitySnapshotFieldNames(record, DIFF_FIELD_NAME);
  }

  public String getCanonicalFieldNames(GenericRecord record) {
    return getEntitySnapshotFieldNames(record, CANONICAL_FIELD_NAME);
  }

  @Override
  public void logFailedRecord(String tableName, GenericRecord record, Throwable t) {
    if (hasCanonicalField(record)) {
      logger.warn(
          String.format(
              "Failed to process record for table: %s including fields \n" + " canonical: %s",
              tableName, getCanonicalFieldNames(record)),
          t);
    } else {
      logger.warn(
          String.format(
              "Failed to process record for table: %s including fields \n"
                  + " pre: %s \n"
                  + " post: %s",
              tableName, getPreFieldNames(record), getDiffFieldNames(record)),
          t);
    }
  }

  /**
   * @param record a CdcRecord
   * @param entitySnapshotFieldName the field from which to extract the EntitySnapshot or EntityDiff
   * @return the entity state in the provided fieldName or an empty map if missing
   */
  private Map<Object, Object> getEntityState(GenericRecord record, String entitySnapshotFieldName) {
    Object entitySnapshot = record.get(entitySnapshotFieldName);
    Map<Object, Object> entityState;
    if (entitySnapshot != null) {
      entityState =
          (Map<Object, Object>) ((GenericRecord) entitySnapshot).get(ENTITY_STATE_FIELD_NAME);
    } else {
      entityState = Collections.emptyMap();
    }
    return entityState;
  }

  /**
   * @param field
   * @return the fields type for required fields or the non null type for optional fields.
   */
  private Schema.Type getType(Schema.Field field) {
    Schema.Type fieldType = field.schema().getType();
    if (field.schema().getType() == Schema.Type.UNION) {
      List<Schema> unionSchemas = field.schema().getTypes();
      if (unionSchemas.size() != 2) {
        throw new IllegalArgumentException("The schema does not support non optional union types");
      }
      Schema.Type firstType = unionSchemas.get(0).getType();
      Schema.Type secondType = unionSchemas.get(1).getType();
      if (firstType == Schema.Type.NULL && secondType != Schema.Type.NULL) {
        return secondType;
      } else if (firstType != Schema.Type.NULL && secondType == Schema.Type.NULL) {
        return firstType;
      } else {
        throw new IllegalArgumentException("The schema does not support non optional union types");
      }
    } else {
      return fieldType;
    }
  }

  /**
   * @param fieldValue
   * @param field
   * @return converts strings or ints that are expected to be longs to longs
   */
  private Object convertValue(Object fieldValue, Schema.Field field) {
    Schema.Type fieldType = getType(field);
    if (fieldValue != null) {
      if (fieldType.equals(Schema.Type.LONG) && !(fieldValue instanceof Long)) {
        BigInteger bi = new BigInteger(fieldValue.toString());
        return bi.longValue();
      }
      {
        return fieldValue;
      }
    } else {
      return fieldValue;
    }
  }

  /**
   * @param field
   * @param entityStates entity states to be checked for values. The values of earlier states are
   *     preferred over later states.
   * @param recordBuilder
   */
  private void addFieldValue(
      Schema.Field field,
      List<Map<Object, Object>> entityStates,
      GenericRecordBuilder recordBuilder) {
    String javaStringFieldName = field.name();
    Utf8 utf8FieldName = new Utf8(field.name());

    Object fieldValue = null;
    for (Map<Object, Object> entityState : entityStates) {
      fieldValue = entityState.get(javaStringFieldName);
      if (fieldValue == null) {
        fieldValue = entityState.get(utf8FieldName);
      }
      if (fieldValue != null) {
        break;
      }
    }

    Object convertedFieldValue = convertValue(fieldValue, field);
    recordBuilder.set(field.name(), convertedFieldValue);
  }

  private GenericRecord createDPMetadata(GenericRecord record, Schema schema) {
    Schema dpMetadataSchema = schema.getField(DP_METADATA_FIELD_NAME).schema();

    GenericRecordBuilder metadataBuilder = new GenericRecordBuilder(dpMetadataSchema);

    GenericRecord eventMetadata = (GenericRecord) record.get(EVENT_METADATA_FIELD_NAME);
    Long eventTimestamp = (Long) eventMetadata.get(EVENT_TIMESTAMP_FIELD_NAME);

    GenericRecord cdcMetadata = (GenericRecord) record.get(CDC_METADATA_FIELD_NAME);
    Long versionEpoch = (Long) cdcMetadata.get(VERSION_EPOCH_FIELD_NAME);
    String shardId;
    Integer cdcShardId = (Integer) cdcMetadata.get(CDC_SHARD_ID_FIELD_NAME);
    if (cdcShardId == null) {
      shardId = null;
    } else {
      shardId = Integer.toString(cdcShardId);
    }

    GenericRecord actorMetadata = (GenericRecord) cdcMetadata.get(ACTOR_METADATA_FIELD_NAME);
    String requestId;
    if (actorMetadata == null) {
      requestId = null;
    } else {
      // call toString instead of casting to String since this field could be a utf8 string
      requestId = actorMetadata.get(CDC_REQUEST_ID_FIELD_NAME).toString();
    }

    String operation = record.get(CDC_OPERATION_FIELD_NAME).toString();

    metadataBuilder.set(VERSION_FIELD_NAME, versionEpoch);
    metadataBuilder.set(DELETED_FIELD_NAME, 0L);
    metadataBuilder.set(DP_OPERATION_FIELD_NAME, operation);
    metadataBuilder.set(EVENT_TIME_FIELD_NAME, eventTimestamp);
    metadataBuilder.set(SHARD_ID_FIELD_NAME, shardId);
    metadataBuilder.set(REQUEST_ID_FIELD_NAME, requestId);
    return metadataBuilder.build();
  }

  /**
   * This is called unwrapped because all exceptions will get wrapped by
   * RecordTransformationException
   *
   * @param record a record in the CDCEvent schema
   * @param schema the entity schema to transform to. Must have dpmeta
   * @return the record transformed to EntitySchema
   */
  @Override
  public GenericRecord unwrappedTransform(GenericRecord record, Schema schema) {
    Schema cdcEventSchema = record.getSchema();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);

    recordBuilder.set(DP_METADATA_FIELD_NAME, createDPMetadata(record, schema));

    List<Map<Object, Object>> entityStates;
    if (hasCanonicalField(record)) {
      entityStates = new ArrayList<>(1);
      entityStates.add(getEntityState(record, CANONICAL_FIELD_NAME));
    } else {
      entityStates = new ArrayList<>(2);
      // the order in which entity states are added to this list matters
      entityStates.add(getEntityState(record, DIFF_FIELD_NAME));
      entityStates.add(getEntityState(record, PRE_FIELD_NAME));
    }
    for (Schema.Field field : schema.getFields()) {
      if (!field.name().equals(DP_METADATA_FIELD_NAME)) {
        addFieldValue(field, entityStates, recordBuilder);
      }
    }
    return recordBuilder.build();
  }

  /**
   * For EntitySchema transformation, the TransformedSchema is the same as the TargetSchema
   *
   * @param currentRecord
   * @param targetSchema
   * @return the targetSchema
   */
  @Override
  public Schema transformSchema(GenericRecord currentRecord, Schema targetSchema) {
    return targetSchema;
  }
}
