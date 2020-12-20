package org.apache.iceberg.addons.testkit;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;

public class IcebergUtils {
  public static List<String> toString(StructLike struct) {
    return IntStream.range(0, struct.size())
        .mapToObj(i -> struct.get(i, Object.class).toString())
        .collect(Collectors.toList());
  }

  private static List<DataFile> getFiles(CloseableIterable<FileScanTask> tasks) {
    return StreamSupport.stream(tasks.spliterator(), true)
        .map(task -> task.file())
        .collect(Collectors.toList());
  }

  public static <T> List<DataFile> getAllDataFilesInRange(
      Table table, Comparable<T> start, Comparable<T> end, String fieldName) {
    table.refresh();

    TableScan tableScan =
        table
            .newScan()
            .filter(
                Expressions.and(
                    Expressions.greaterThanOrEqual(fieldName, start),
                    Expressions.lessThan(fieldName, end)));

    return getFiles(tableScan.planFiles());
  }

  public static List<DataFile> getAllDataFiles(Table table) {
    table.refresh();

    TableScan tableScan = table.newScan();

    return getFiles(tableScan.planFiles());
  }

  public static List<GenericRecord> getAllGenericRecords(Table table) {
    return getAllGenericRecords(table, getAllDataFiles(table));
  }

  public static List<GenericRecord> getAllGenericRecords(Table table, List<DataFile> dataFiles) {
    return dataFiles
        .parallelStream()
        .flatMap(df -> getAllGenericRecordsStream(table, df))
        .collect(Collectors.toList());
  }

  public static List<GenericRecord> getAllGenericRecords(Table table, DataFile dataFile) {
    return getAllGenericRecordsStream(table, dataFile).collect(Collectors.toList());
  }

  private static Stream<GenericRecord> getAllGenericRecordsStream(Table table, DataFile dataFile) {
    EncryptionManager encryptionManager = table.encryption();
    EncryptedInputFile encryptedFile =
        EncryptedFiles.encryptedInput(
            table.io().newInputFile(dataFile.path().toString()), dataFile.keyMetadata());

    InputFile decryptedFile = encryptionManager.decrypt(encryptedFile);

    AvroIterable<GenericRecord> avroIterable =
        Avro.read(decryptedFile).project(table.schema()).build();

    return StreamSupport.stream(avroIterable.spliterator(), true);
  }
}
