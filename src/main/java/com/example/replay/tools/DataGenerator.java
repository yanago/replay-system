package com.example.replay.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Standalone data generator that writes 50 000+ synthetic security events to
 * an Apache Iceberg table partitioned by day.
 *
 * <p>Usage (from the repo root):
 * <pre>
 *   java -cp target/replay-system-*.jar \
 *        com.example.replay.tools.DataGenerator ./data/security_events
 * </pre>
 * Or via the helper script: {@code scripts/generate-data.sh}
 *
 * <h3>Customer skew</h3>
 * Events are distributed across 100 customers using a Zipf-like distribution:
 * customer-001 gets ≈18 % of all events, customer-002 ≈9 %, …, so the top-10
 * customers receive roughly 60 % of total traffic.
 *
 * <h3>Output layout</h3>
 * <pre>
 *   {tableLocation}/
 *     metadata/
 *       v1.metadata.json
 *       …
 *     data/
 *       event_time_day=19723/   ← days since epoch (2024-01-01)
 *         *.parquet
 *       event_time_day=19724/
 *         *.parquet
 *       …
 * </pre>
 */
public final class DataGenerator {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    // -----------------------------------------------------------------------
    // Configuration constants
    // -----------------------------------------------------------------------

    private static final int    NUM_CUSTOMERS    = 100;
    private static final int    TOTAL_EVENTS     = 55_000;  // > 50k
    private static final int    WRITE_BATCH_SIZE = 5_000;   // events per Parquet file

    private static final LocalDate START_DATE = LocalDate.of(2026, 1, 1);
    private static final int       NUM_DAYS   = 30;

    private static final String[] EVENT_TYPES = {
        "LOGIN_SUCCESS", "LOGIN_FAILURE", "FILE_ACCESS", "FILE_MODIFY", "FILE_DELETE",
        "NETWORK_ALERT", "PORT_SCAN", "LATERAL_MOVEMENT", "DATA_EXFIL",
        "PRIVILEGE_ESCALATION", "PROCESS_CREATE", "REGISTRY_MODIFY"
    };

    // Weighted: LOW=40%, MEDIUM=35%, HIGH=20%, CRITICAL=5%
    private static final String[] SEVERITY_POOL = buildPool(
            new String[]{"LOW", "MEDIUM", "HIGH", "CRITICAL"},
            new int[]{40, 35, 20, 5});

    private static final String[] SOURCE_IP_PREFIXES = {
        "10.0.1.", "10.0.2.", "192.168.1.", "192.168.2.", "172.16.0."
    };

    private static final String[] TARGET_HOSTS = {
        "db-primary", "db-replica", "api-gateway", "auth-service",
        "data-export", "admin-panel", "kafka-broker", "file-server",
        "backup-host", "monitoring"
    };

    // -----------------------------------------------------------------------
    // Entry point
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String tableLocation = args.length > 0 ? args[0] : "data/security_events";
        new DataGenerator().generate(Path.of(tableLocation).toAbsolutePath().toString());
    }

    // -----------------------------------------------------------------------
    // Generator
    // -----------------------------------------------------------------------

    private final Random rng = new Random(42L);  // fixed seed for reproducibility

    /** Pre-computed Zipf weights (cumulative, normalised). */
    private final double[] zipfCdf = buildZipfCdf(NUM_CUSTOMERS);

    void generate(String tableLocation) throws Exception {
        log.info("Generating {} events → {}", TOTAL_EVENTS, tableLocation);

        var conf   = new Configuration();
        var tables = new HadoopTables(conf);

        // Delete existing table so the generator is idempotent
        var tableDir = new File(tableLocation);
        if (tableDir.exists()) {
            log.info("Removing existing table at {}", tableLocation);
            deleteRecursively(tableDir);
        }

        var schema = buildSchema();
        var spec   = PartitionSpec.builderFor(schema).day("event_time").build();
        var table  = tables.create(schema, spec, tableLocation);

        log.info("Iceberg table created with schema:\n{}", schema);

        // Distribute events uniformly across days
        int eventsPerDay = TOTAL_EVENTS / NUM_DAYS;
        int remainder    = TOTAL_EVENTS % NUM_DAYS;

        int totalWritten = 0;
        var appendOp     = table.newAppend();

        for (int d = 0; d < NUM_DAYS; d++) {
            LocalDate date      = START_DATE.plusDays(d);
            int       dayEvents = eventsPerDay + (d < remainder ? 1 : 0);

            totalWritten += writeDay(table, schema, date, dayEvents, appendOp);
        }

        appendOp.commit();
        log.info("Done. Total events written: {}", totalWritten);
    }

    /** Writes all events for one calendar day and adds their DataFiles to {@code appendOp}. */
    private int writeDay(
            Table table, Schema schema,
            LocalDate date, int numEvents,
            org.apache.iceberg.AppendFiles appendOp) throws Exception {

        String partPath   = "event_time_day=" + date;

        List<GenericRecord> buffer = new ArrayList<>(WRITE_BATCH_SIZE);
        int written = 0;

        for (int i = 0; i < numEvents; i++) {
            buffer.add(buildRecord(schema, date));

            if (buffer.size() >= WRITE_BATCH_SIZE || i == numEvents - 1) {
                DataFile dataFile = writeParquetFile(table, schema, partPath, buffer);
                appendOp.appendFile(dataFile);
                written += buffer.size();
                buffer.clear();
            }
        }

        log.info("Day {} ({}) — {} events", date, partPath, written);
        return written;
    }

    /** Writes a list of records to one Parquet file and returns the DataFile descriptor. */
    private DataFile writeParquetFile(
            Table table, Schema schema,
            String partPath, List<GenericRecord> records) throws Exception {

        String fileName = UUID.randomUUID() + ".parquet";
        String location = table.location() + "/data/" + partPath + "/" + fileName;

        var outputFile = table.io().newOutputFile(location);

        FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        try {
            for (var rec : records) appender.add(rec);
        } finally {
            appender.close();
        }

        return DataFiles.builder(table.spec())
                .withFormat(FileFormat.PARQUET)
                .withInputFile(outputFile.toInputFile())
                .withMetrics(appender.metrics())
                .withPartitionPath(partPath)
                .build();
    }

    // -----------------------------------------------------------------------
    // Record builder
    // -----------------------------------------------------------------------

    private GenericRecord buildRecord(Schema schema, LocalDate date) {
        var rec = GenericRecord.create(schema);

        // Random second within the day
        int secondOfDay = rng.nextInt(86_400);
        var eventTime   = LocalDateTime.of(date, LocalTime.ofSecondOfDay(secondOfDay))
                .toInstant(ZoneOffset.UTC);
        // Ingestion timestamp: 0–300 seconds after the event
        var ingestTime  = eventTime.plusSeconds(rng.nextInt(301));

        rec.setField("event_id",        UUID.randomUUID().toString());
        rec.setField("cid",             sampleCustomer());
        rec.setField("event_timestamp", LocalDateTime.ofInstant(ingestTime, ZoneOffset.UTC));
        rec.setField("event_time",      LocalDateTime.ofInstant(eventTime,  ZoneOffset.UTC));
        rec.setField("event_type",      EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)]);
        rec.setField("source_ip",       SOURCE_IP_PREFIXES[rng.nextInt(SOURCE_IP_PREFIXES.length)]
                                        + (rng.nextInt(254) + 1));
        rec.setField("target_host",     TARGET_HOSTS[rng.nextInt(TARGET_HOSTS.length)]);
        rec.setField("severity",        SEVERITY_POOL[rng.nextInt(SEVERITY_POOL.length)]);

        return rec;
    }

    /** Samples a customer ID with Zipf distribution (lower index = more events). */
    private String sampleCustomer() {
        double r = rng.nextDouble();
        for (int i = 0; i < NUM_CUSTOMERS; i++) {
            if (r < zipfCdf[i]) return "cust-%03d".formatted(i + 1);
        }
        return "cust-%03d".formatted(NUM_CUSTOMERS);
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    private static Schema buildSchema() {
        return new Schema(
                Types.NestedField.required(1, "event_id",        Types.StringType.get()),
                Types.NestedField.required(2, "cid",             Types.StringType.get()),
                Types.NestedField.required(3, "event_timestamp", Types.TimestampType.withoutZone()),
                Types.NestedField.required(4, "event_time",      Types.TimestampType.withoutZone()),
                Types.NestedField.required(5, "event_type",      Types.StringType.get()),
                Types.NestedField.optional(6, "source_ip",       Types.StringType.get()),
                Types.NestedField.optional(7, "target_host",     Types.StringType.get()),
                Types.NestedField.optional(8, "severity",        Types.StringType.get())
        );
    }

    // -----------------------------------------------------------------------
    // Static helpers
    // -----------------------------------------------------------------------

    /** Builds a cumulative distribution array for Zipf with {@code n} items. */
    private static double[] buildZipfCdf(int n) {
        double[] cdf = new double[n];
        double   sum = 0;
        for (int i = 0; i < n; i++) sum += 1.0 / (i + 1);
        double cumulative = 0;
        for (int i = 0; i < n; i++) {
            cumulative += 1.0 / ((i + 1) * sum);
            cdf[i] = cumulative;
        }
        cdf[n - 1] = 1.0;  // guard against floating-point rounding
        return cdf;
    }

    /** Expands {value, weight} pairs into a flat pool for uniform sampling. */
    private static String[] buildPool(String[] values, int[] weights) {
        var list = new ArrayList<String>();
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < weights[i]; j++) list.add(values[i]);
        }
        return list.toArray(new String[0]);
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) for (File c : f.listFiles()) deleteRecursively(c);
        f.delete();
    }
}
