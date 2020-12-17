package com.box.dataplatform.iceberg.client.model;

import org.apache.iceberg.StructLike;

/**
 * This class wraps the iceberg StructLike which contains concrete partition information for a
 * Record/DataFile.
 *
 * <p>The extra functionality is to return a PartitionIdentifier with the constraint that for two
 * PartitionInfos, the PartitionIdentifier is the same iff the partition data is the same. This will
 * help clients batch records in the same partition.
 */
public class PartitionInfo {
  private static final String DEFAULT_PARTITION_DELIMITER = ".";
  private final StructLike partitionData;

  public PartitionInfo(StructLike partitionData) {
    if (partitionData == null) {
      throw new IllegalArgumentException("Partition data must not be null");
    }
    this.partitionData = partitionData;
  }

  public StructLike getPartitionData() {
    return partitionData;
  }

  public String getPartitionIdentifier() {
    return getPartitionIdentifier(DEFAULT_PARTITION_DELIMITER);
  }

  public String getPartitionIdentifier(String delimiter) {
    Integer partitionValue = partitionData.get(0, Integer.class);
    StringBuilder resultBuilder = new StringBuilder();
    for (int i = 0; i < partitionData.size(); i++) {
      String partitionStringValue = partitionData.get(i, Object.class).toString();
      resultBuilder.append(partitionStringValue);
      resultBuilder.append(delimiter);
    }
    return resultBuilder.toString();
  }
}
