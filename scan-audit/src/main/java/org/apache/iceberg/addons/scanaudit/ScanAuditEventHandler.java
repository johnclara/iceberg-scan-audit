package org.apache.iceberg.addons.scanaudit;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class ScanAuditEventHandler {
  private static final Logger log = LoggerFactory.getLogger(ScanAuditEventHandler.class);
  private final Table table;
  private final ConcurrentHashMap<Long, FileAppender<Record>> openFiles;
  private final FileFormat fileFormat;
  private final GenericAppenderFactory appenderFactory;
  private boolean closed = false;

  public ScanAuditEventHandler(Table table) {
    this.table = table;
    openFiles = new ConcurrentHashMap<>();
    fileFormat = FileFormat.valueOf(table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH));
    appenderFactory = new GenericAppenderFactory(table.schema());
  }

  public Listener<ScanAuditStartEvent> startListener() {
    return new Listener<ScanAuditStartEvent>() {
      @Override
      public void notify(ScanAuditStartEvent event) {
        log.warn("{}", event.toString());
        if (!closed) {
          openFiles.compute(event.scanId(), (k, v) -> {
            if (v != null) {
              throw new IllegalStateException("Scan already started");
            } else {
              OutputFile outputFile = table.io().newOutputFile(
                  table.locationProvider()
                      .newDataLocation(fileFormat.addExtension(String.format("scan-%d", k)))
              );

              return appenderFactory
                  .newAppender(
                      outputFile,
                      fileFormat
                  );
              }
          });
        }
      }
    };
  }

  public Listener<ScanAuditEvaluateEvent> evaluateListener() {
    return new Listener<ScanAuditEvaluateEvent>() {
      @Override
      public void notify(ScanAuditEvaluateEvent event) {
        log.warn("{}", event.toString());
        if (!closed) {
          FileAppender<Record> recordFileAppender = openFiles.get(event.scanId());
          if (recordFileAppender != null) {
            recordFileAppender.add(event.toRecord(table.schema()));
          }
        }
      }
    };
  }


  public Listener<ScanAuditEndEvent> endListener() {
    return new Listener<ScanAuditEndEvent>() {
      @Override
      public void notify(ScanAuditEndEvent event) {
        log.warn("{}", event.toString());
        if (!closed) {
          FileAppender<Record> openFile = openFiles.get(event.scanId());
          if (openFile != null) {
            openFiles.remove(event.scanId());
            try {
              openFile.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
            String location = table.locationProvider().newDataLocation(fileFormat.addExtension(String.format("scan-%d", event.scanId())));
            DataFile df = DataFiles.builder(table.spec())
                .withInputFile(table.io().newInputFile(location))
                .withMetrics(openFile.metrics())
                .build();
            table.newFastAppend().appendFile(df).commit();
          }
        }
      }
    };
  }

  public void register() {
    Listeners.register(this.startListener(), ScanAuditStartEvent.class);
    Listeners.register(this.evaluateListener(), ScanAuditEvaluateEvent.class);
    Listeners.register(this.endListener(), ScanAuditEndEvent.class);
  }

  public void close() {
    closed = true;
  }
}
