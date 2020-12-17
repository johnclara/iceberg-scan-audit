package com.box.dataplatform.iceberg.core.testkit.dynamo;

import static com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit.ICEBERG_DYNAMO_TABLE;
import static com.box.dataplatform.iceberg.core.testkit.util.TestkitUtils.getAndCheckProperty;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.dynamodb.DynamoClientFactory;
import com.box.dataplatform.iceberg.core.metastore.DynamoDBIcebergTableMetadata;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoTestkit extends DynamoClientFactory {
  private static final Logger log = LoggerFactory.getLogger(DynamoTestkit.class);

  private static AmazonDynamoDB amazonDynamoDB = null;

  /** This will initialize the local dynamo with an IcebergMetadataTable. */
  public static AmazonDynamoDB refresh() {
    log.info("Refreshing embedded mock dynamo");
    String sqlite4javaLibPath = getAndCheckProperty("sqlite4java.library.path");
    amazonDynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();

    DynamoDBMapper mapper = new DynamoDBMapper(DynamoTestkit.get());

    CreateTableRequest req =
        mapper.generateCreateTableRequest(
            DynamoDBIcebergTableMetadata.class,
            new DynamoDBMapperConfig(
                DynamoDBMapperConfig.SaveBehavior.UPDATE,
                DynamoDBMapperConfig.ConsistentReads.CONSISTENT,
                new DynamoDBMapperConfig.TableNameOverride(ICEBERG_DYNAMO_TABLE)));
    // Table provision throughput is still required since it cannot be specified in your POJO
    req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
    DynamoTestkit.get().createTable(req);

    return amazonDynamoDB;
  }

  public static AmazonDynamoDB get() {
    if (amazonDynamoDB == null) {
      return refresh();
    } else {
      return amazonDynamoDB;
    }
  }

  public static AmazonDynamoDB wrap(Function<AmazonDynamoDB, AmazonDynamoDB> wrapper) {
    amazonDynamoDB = wrapper.apply(amazonDynamoDB);
    return amazonDynamoDB;
  }

  public static void stop() {
    amazonDynamoDB = null;
  }

  /** Empty public constructor required for reflection */
  public DynamoTestkit() {}

  public AmazonDynamoDB create(AwsClientConfig awsClientConfig) {
    return amazonDynamoDB;
  }
}
