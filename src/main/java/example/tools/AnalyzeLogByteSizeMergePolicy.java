package example.tools;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This test simulates continuous indexing over time, where newer documents have newer timestamps.
 * It demonstrates how segment creation correlates with timestamp ordering in a real-world scenario.
 */
public class AnalyzeLogByteSizeMergePolicy {

    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws IOException {
        // Create a temporary directory for the index
        Path indexPath = Files.createTempDirectory("lucene-continuous-indexing-test");
        System.out.println("Index path: " + indexPath);

        try {
            Directory directory = FSDirectory.open(indexPath);
            MergePolicy mergePolicy = new LogByteSizeMergePolicy();

            // Simulate continuous indexing over several "days"
            simulateContinuousIndexing(directory, mergePolicy);

            // Examine the index after continuous indexing
            System.out.println("\n=== Index after continuous indexing ===");
            examineIndex(directory);

            // Force merge to 1 segment
            forceMergeIndex(directory, mergePolicy, 1);

            // Examine after force merge
            System.out.println("\n=== Index after force merge to 1 segment ===");
            examineIndex(directory);

            // Close resources
            directory.close();
        } catch (Exception e) {
            System.err.println("Error during test: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Simulates continuous indexing over time, where each "day" of indexing adds
     * newer documents with increasing timestamps.
     */
    private static void simulateContinuousIndexing(Directory directory, MergePolicy mergePolicy) throws IOException {
        System.out.println("\n=== Simulating continuous indexing over time ===");

        // Base timestamp for our simulation (start from now)
        long baseTimestamp = System.currentTimeMillis();

        // Create a writer with the merge policy
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMergePolicy(mergePolicy);

        // We'll use a moderate buffer size to allow some natural segment formation
        config.setMaxBufferedDocs(50);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            // Simulate 5 "days" of indexing
            for (int day = 0; day < 5; day++) {
                System.out.println("Indexing day " + day + "...");

                // Each "day" we add 100 documents with increasing timestamps
                for (int docInDay = 0; docInDay < 100; docInDay++) {
                    // Calculate a timestamp that increases with each document
                    // Each "day" is offset by 24 hours (86400000 ms)
                    long timestamp = baseTimestamp + (day * 86400000) + (docInDay * 60000); // Each doc is 1 minute apart

                    Document doc = new Document();
                    doc.add(new StringField("day", "day" + day, Field.Store.YES));
                    doc.add(new StringField("docInDay", String.valueOf(docInDay), Field.Store.YES));
                    doc.add(new LongPoint(TIMESTAMP_FIELD, timestamp));
                    doc.add(new NumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                    doc.add(new StoredField(TIMESTAMP_FIELD, timestamp));

                    writer.addDocument(doc);

                    // Commit every 20 documents to force some segment creation
                    if (docInDay > 0 && docInDay % 20 == 0) {
                        writer.commit();
                        System.out.println("  Committed batch " + (docInDay / 20) + " of day " + day);
                    }
                }

                // Always commit at the end of each "day"
                writer.commit();
                System.out.println("  Completed indexing for day " + day);

                // Print segment count at this point
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    System.out.println("  Number of segments after day " + day + ": " + reader.leaves().size());
                }
            }
        }
    }

    private static void forceMergeIndex(Directory directory, MergePolicy mergePolicy, int maxSegments) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMergePolicy(mergePolicy);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            System.out.println("Force merging to " + maxSegments + " segments...");
            writer.forceMerge(maxSegments);
        }
    }

    private static void examineIndex(Directory directory) throws IOException {
        try (IndexReader reader = DirectoryReader.open(directory)) {
            System.out.println("Number of segments: " + reader.leaves().size());

            int segmentNum = 0;
            for (LeafReaderContext leafContext : reader.leaves()) {
                System.out.println("\n--- Segment " + segmentNum + " ---");
                System.out.println("Document count: " + leafContext.reader().maxDoc());

                // Check timestamp range for this segment
                analyzeSegmentTimestampRange(leafContext, TIMESTAMP_FIELD);

                // Show sample documents from beginning, middle and end of segment
                sampleDocuments(leafContext, TIMESTAMP_FIELD);

                segmentNum++;
            }
        }
    }

    private static void analyzeSegmentTimestampRange(LeafReaderContext leafContext, String field) throws IOException {
        NumericDocValues docValues = leafContext.reader().getNumericDocValues(field);
        if (docValues == null) {
            System.out.println("No numeric doc values for field: " + field);
            return;
        }

        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        String minDay = "";
        String maxDay = "";

        // Find min and max timestamps in this segment
        for (int i = 0; i < leafContext.reader().maxDoc(); i++) {
            if (docValues.advanceExact(i)) {
                long timestamp = docValues.longValue();
                Document doc = leafContext.reader().storedFields().document(i);

                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                    minDay = doc.get("day");
                }

                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                    maxDay = doc.get("day");
                }
            }
        }

        if (minTimestamp != Long.MAX_VALUE && maxTimestamp != Long.MIN_VALUE) {
            System.out.println("Timestamp range:");
            System.out.println("  Min: " + minTimestamp + " (" + DATE_FORMAT.format(new Date(minTimestamp)) + ") from " + minDay);
            System.out.println("  Max: " + maxTimestamp + " (" + DATE_FORMAT.format(new Date(maxTimestamp)) + ") from " + maxDay);
            System.out.println("  Time span: " + ((maxTimestamp - minTimestamp) / (60 * 1000)) + " minutes");
        } else {
            System.out.println("No timestamp values found in this segment");
        }
    }

    private static void sampleDocuments(LeafReaderContext leafContext, String field) throws IOException {
        int maxDoc = leafContext.reader().maxDoc();
        if (maxDoc == 0) return;

        System.out.println("\nAll documents in this segment:");
        System.out.println("DocID | Timestamp | Date");
        System.out.println("----------------------------------------------");

        NumericDocValues docValues = leafContext.reader().getNumericDocValues(field);
        if (docValues == null) {
            System.out.println("No numeric doc values for field: " + field);
            return;
        }

        // Print all documents (limited to first 500 to avoid excessive output)
        int limit = Math.min(500, maxDoc);
        for (int position = 0; position < limit; position++) {
            if (docValues.advanceExact(position)) {
                long timestamp = docValues.longValue();
                String date = DATE_FORMAT.format(new Date(timestamp));

                System.out.printf("%5d | %d | %s%n",
                        position, timestamp, date);
            } else {
                System.out.printf("%5d | No timestamp value%n", position);
            }
        }

        // If we hit the limit, show that there are more docs
        if (maxDoc > limit) {
            System.out.println("... " + (maxDoc - limit) + " more documents not shown ...");
        }
    }
}
