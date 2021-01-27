package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.iceberg.addons.scanaudit.EvaluatorType;
import org.apache.iceberg.addons.scanaudit.ScanAuditEvaluateEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;

class AuditingManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  // TODO ADDING VARIABLES
  private final String tableName;
  private final long scanId;
  private final String manifestListPath;
  // TODO ADDING VARIABLES END
  private final FileIO io;
  private final Set<ManifestFile> dataManifests;
  private final DeleteFileIndex.Builder deleteIndexBuilder;
  private Predicate<ManifestFile> manifestPredicate;
  private Predicate<ManifestEntry<DataFile>> manifestEntryPredicate;
  private Map<Integer, PartitionSpec> specsById;
  private Expression dataFilter;
  private Expression fileFilter;
  private Expression partitionFilter;
  private boolean ignoreDeleted;
  private boolean ignoreExisting;
  private boolean ignoreResiduals;
  private List<String> columns;
  private boolean caseSensitive;
  private ExecutorService executorService;

  // TODO ADDING PARAM
  AuditingManifestGroup(String tableName, long scanId, String manifestListPath, FileIO io, Iterable<ManifestFile> manifests) {
    this(tableName,
        scanId,
        manifestListPath,
        io,
        // TODO ADDING PARAM END
        Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DATA),
        Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DELETES));
  }

  // TODO ADDING PARAM
  AuditingManifestGroup(String tableName, long scanId, String manifestListPath, FileIO io, Iterable<ManifestFile> dataManifests, Iterable<ManifestFile> deleteManifests) {
    this.tableName = tableName;
    this.scanId = scanId;
    this.manifestListPath = manifestListPath;
    // TODO ADDING PARAM END
    this.io = io;
    this.dataManifests = Sets.newHashSet(dataManifests);
    this.deleteIndexBuilder = DeleteFileIndex.builderFor(io, deleteManifests);
    this.dataFilter = Expressions.alwaysTrue();
    this.fileFilter = Expressions.alwaysTrue();
    this.partitionFilter = Expressions.alwaysTrue();
    this.ignoreDeleted = false;
    this.ignoreExisting = false;
    this.ignoreResiduals = false;
    this.columns = ManifestReader.ALL_COLUMNS;
    this.caseSensitive = true;
    this.manifestPredicate = m -> true;
    this.manifestEntryPredicate = e -> true;
  }

  AuditingManifestGroup specsById(Map<Integer, PartitionSpec> newSpecsById) {
    this.specsById = newSpecsById;
    deleteIndexBuilder.specsById(newSpecsById);
    return this;
  }

  AuditingManifestGroup filterData(Expression newDataFilter) {
    this.dataFilter = Expressions.and(dataFilter, newDataFilter);
    deleteIndexBuilder.filterData(newDataFilter);
    return this;
  }

  AuditingManifestGroup filterFiles(Expression newFileFilter) {
    this.fileFilter = Expressions.and(fileFilter, newFileFilter);
    return this;
  }

  AuditingManifestGroup filterPartitions(Expression newPartitionFilter) {
    this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
    deleteIndexBuilder.filterPartitions(newPartitionFilter);
    return this;
  }

  AuditingManifestGroup filterManifests(Predicate<ManifestFile> newManifestPredicate) {
    this.manifestPredicate = manifestPredicate.and(newManifestPredicate);
    return this;
  }

  AuditingManifestGroup filterManifestEntries(Predicate<ManifestEntry<DataFile>> newManifestEntryPredicate) {
    this.manifestEntryPredicate = manifestEntryPredicate.and(newManifestEntryPredicate);
    return this;
  }

  AuditingManifestGroup ignoreDeleted() {
    this.ignoreDeleted = true;
    return this;
  }

  AuditingManifestGroup ignoreExisting() {
    this.ignoreExisting = true;
    return this;
  }

  AuditingManifestGroup ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  AuditingManifestGroup select(List<String> newColumns) {
    this.columns = Lists.newArrayList(newColumns);
    return this;
  }

  AuditingManifestGroup caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    deleteIndexBuilder.caseSensitive(newCaseSensitive);
    return this;
  }

  AuditingManifestGroup planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    deleteIndexBuilder.planWith(newExecutorService);
    return this;
  }

  /**
   * Returns a iterable of scan tasks. It is safe to add entries of this iterable
   * to a collection as {@link DataFile} in each {@link FileScanTask} is defensively
   * copied.
   * @return a {@link CloseableIterable} of {@link FileScanTask}
   */
  public CloseableIterable<FileScanTask> planFiles() {
    LoadingCache<Integer, ResidualEvaluator> residualCache = Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = specsById.get(specId);
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
      return ResidualEvaluator.of(spec, filter, caseSensitive);
    });

    DeleteFileIndex deleteFiles = deleteIndexBuilder.build();

    boolean dropStats = ManifestReader.dropStats(dataFilter, columns);
    if (!deleteFiles.isEmpty()) {
      select(Streams.concat(columns.stream(), ManifestReader.STATS_COLUMNS.stream()).collect(Collectors.toList()));
    }

    Iterable<CloseableIterable<FileScanTask>> tasks = entries((manifest, entries) -> {
      int specId = manifest.partitionSpecId();
      PartitionSpec spec = specsById.get(specId);
      String schemaString = SchemaParser.toJson(spec.schema());
      String specString = PartitionSpecParser.toJson(spec);
      ResidualEvaluator residuals = residualCache.get(specId);
      if (dropStats) {
        return CloseableIterable.transform(entries, e -> new BaseFileScanTask(
            e.file().copyWithoutStats(), deleteFiles.forEntry(e), schemaString, specString, residuals));
      } else {
        return CloseableIterable.transform(entries, e -> new BaseFileScanTask(
            e.file().copy(), deleteFiles.forEntry(e), schemaString, specString, residuals));
      }
    });

    if (executorService != null) {
      return new ParallelIterable<>(tasks, executorService);
    } else {
      return CloseableIterable.concat(tasks);
    }
  }

  /**
   * Returns an iterable for manifest entries in the set of manifests.
   * <p>
   * Entries are not copied and it is the caller's responsibility to make defensive copies if
   * adding these entries to a collection.
   *
   * @return a CloseableIterable of manifest entries.
   */
  public CloseableIterable<ManifestEntry<DataFile>> entries() {
    return CloseableIterable.concat(entries((manifest, entries) -> entries));
  }

  private <T> Iterable<CloseableIterable<T>> entries(
      BiFunction<ManifestFile, CloseableIterable<ManifestEntry<DataFile>>, CloseableIterable<T>> entryFn) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = specsById == null ?
        null : Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = specsById.get(specId);
      return ManifestEvaluator.forPartitionFilter(
          Expressions.and(partitionFilter, Projections.inclusive(spec, caseSensitive).project(dataFilter)),
          spec, caseSensitive);
    });

    Evaluator evaluator;
    if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
      evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter, caseSensitive);
    } else {
      evaluator = null;
    }

    // TODO added event
    Stream<ManifestFile> matchingManifests = evalCache == null ? dataManifests.stream() :
        dataManifests.stream().filter(manifest -> {
          boolean passedEvaluation = evalCache.get(manifest.partitionSpecId()).eval(manifest);
          Listeners.notifyAll(ScanAuditEvaluateEvent.of(tableName, scanId, manifestListPath, manifest.path(), EvaluatorType.MANIFEST_PARTITION, passedEvaluation));
          return passedEvaluation;
        });
    // TODO added event end

    if (ignoreDeleted) {
      // only scan manifests that have entries other than deletes
      // remove any manifests that don't have any existing or added files. if either the added or
      // existing files count is missing, the manifest must be scanned.
      matchingManifests = matchingManifests.filter(manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
    }

    if (ignoreExisting) {
      // only scan manifests that have entries other than existing
      // remove any manifests that don't have any deleted or added files. if either the added or
      // deleted files count is missing, the manifest must be scanned.
      matchingManifests = matchingManifests.filter(manifest -> manifest.hasAddedFiles() || manifest.hasDeletedFiles());
    }

    // TODO added event
    matchingManifests = matchingManifests.filter(manifest -> {
      boolean passedEvaluation = manifestPredicate.test(manifest);
      Listeners.notifyAll(ScanAuditEvaluateEvent.of(tableName, scanId, manifestListPath, manifest.path(), EvaluatorType.MANIFEST_PREDICATE, passedEvaluation));
      return passedEvaluation;
    });
    // TODO added event end
    Iterable<ManifestFile> cached = matchingManifests.collect(Collectors.toList());

    return Iterables.transform(
        cached,
        manifest -> {
          // TODO added manifest reader
          InputFile file = io.newInputFile(manifest.path());
          InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest);
          AuditedManifestReader<DataFile> reader = new AuditedManifestReader<DataFile>(tableName, scanId, file, specsById, inheritableMetadata, AuditedManifestReader.FileType.DATA_FILES)
              .filterRows(dataFilter)
              .filterPartitions(partitionFilter)
              .caseSensitive(caseSensitive)
              .select(columns);
          // TODO end added manifest reader

          CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
          if (ignoreDeleted) {
            entries = reader.liveEntries();
          }

          if (ignoreExisting) {
            entries = CloseableIterable.filter(entries,
                entry -> entry.status() != ManifestEntry.Status.EXISTING);
          }

          // TODO added event
          if (evaluator != null) {
            entries = CloseableIterable.filter(entries, entry -> {
              boolean passedEvaluation = evaluator.eval((GenericDataFile) entry.file());
              Listeners.notifyAll(ScanAuditEvaluateEvent.of(tableName, scanId, manifest.path(), entry.file().path().toString(), EvaluatorType.PARTITION_REEVALUATE, passedEvaluation));
              return passedEvaluation;
             });
          }
          // TODO added event

          // TODO added event
          entries = CloseableIterable.filter(entries, entry -> {
            boolean passedEvaluation = manifestEntryPredicate.test(entry);
            Listeners.notifyAll(ScanAuditEvaluateEvent.of(tableName, scanId, manifest.path(), entry.file().path().toString(), EvaluatorType.PREDICATE_REEVALUATE, passedEvaluation));
            return passedEvaluation;
          });
          // TODO added event

          return entryFn.apply(manifest, entries);
        });
  }
}
