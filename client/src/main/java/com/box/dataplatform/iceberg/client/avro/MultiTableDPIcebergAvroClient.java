package com.box.dataplatform.iceberg.client.avro;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.iceberg.client.MultiTableDPIcebergClientImpl;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;

public class MultiTableDPIcebergAvroClient extends MultiTableDPIcebergClientImpl<GenericRecord> {
  public MultiTableDPIcebergAvroClient(
      String tenantName,
      List<String> tableNames,
      AwsClientConfig awsClientConfig,
      String kmsKeyId,
      TimeUnit timeUnit,
      boolean useFastAppend) {
    super(
        tableNames,
        (tableName) ->
            new DPIcebergAvroClient(
                tenantName, tableName, awsClientConfig, kmsKeyId, timeUnit, useFastAppend));
  }
}
