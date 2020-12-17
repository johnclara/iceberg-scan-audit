package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.CloseableIterable;

/** */
public class DPManifestUtils {
  /**
   * read manifest group and extract partitions from each data file.
   *
   * @param table
   * @param manifestFile
   * @return list of partitions from data file.
   */
  public static List<StructLike> processManifest(
      Table table, ManifestFile manifestFile, Set<Long> newSnapshots) {
    List<StructLike> partitions = new ArrayList<>();
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        new ManifestGroup(table.io(), java.util.Arrays.asList(manifestFile))
            .select(
                ImmutableList.of(
                    "file_path", "file_format", "partition", "record_count", "file_size_in_bytes"))
            .entries()) {

      for (ManifestEntry entry : entries) {
        ContentFile contentFile = entry.file();
        // only add the data file if it is from a an unseen snapshot
        if (newSnapshots.contains(entry.snapshotId())) {
          partitions.add(((ContentFile) contentFile.copy()).partition());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close entries while caching changes", e);
    }
    return partitions;
  }
}
