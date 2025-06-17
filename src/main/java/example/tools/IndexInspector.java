package example.tools;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class IndexInspector {

    public static void main(String[] args) throws Exception {
        // Path for the index that has regression issues
        Path indexPathWithRegression = Paths.get("/home/ec2-user/OS_30/OS_30_27_Vanilla_968eafbd37ef0b864e887643c74291cc3e5ca0d0/nodes/0/indices/rOA5QvdyQ-2JYf3h0WZmFw/0/index");
        // Path indexPathWithoutRegression = Paths.get("/home/ec2-user/OS_30/OS_2191_1_2e4741fb45d1b150aaeeadf66d41445b23ff5982/nodes/0/indices/Id-ddMW1RTqSkiHeqTbCRA/0/index");
        String fieldName = "@timestamp";
        System.out.println("\n\n=== INSPECTING INDEX WITH REGRESSION ===");
        inspectIndex(indexPathWithRegression, fieldName);
    }

    private static void inspectIndex(Path indexPath, String fieldName) throws IOException {

        try (FSDirectory directory = FSDirectory.open(indexPath);
             IndexReader reader = DirectoryReader.open(directory)) {

            for (LeafReaderContext leaf : reader.leaves()) {
                System.out.println("\n--- Document Order Analysis ---");
                printDocumentsInOrder(leaf, fieldName);

                System.out.println("\n--- listAvailableFields ---");
                listAvailableFields(leaf);

                System.out.println("\n--- listPointValueFields ---");
                listPointValueFields(leaf);

                System.out.println("\n--- listDocValueFields ---");
                listDocValueFields(leaf);
            }
        }
    }

    // New method to print documents in order of their timestamp values
    private static void printDocumentsInOrder(LeafReaderContext leaf, String fieldName) throws IOException {

        // First check if this field exists
        FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(fieldName);
        if (fieldInfo == null) {
            System.out.println("Field '" + fieldName + "' does not exist in this segment");
            return;
        }

        SortedNumericDocValues docValues = leaf.reader().getSortedNumericDocValues(fieldName);
        if (docValues == null) {
            System.out.println("No sorted numeric doc values for field '" + fieldName + "'");
            return;
        }

        // Collect all documents and their timestamp values
        List<DocTimestamp> docTimestamps = new ArrayList<>();

        // Iterate through all docs that have this field
        int docId;
        while ((docId = docValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            // SortedNumericDocValues may have multiple values per document
            // For timestamp we typically take the first one
            if (docValues.docValueCount() > 0) {
                long timestamp = docValues.nextValue();
                docTimestamps.add(new DocTimestamp(docId, timestamp));
            }
        }

        // Print number of documents found
        System.out.println("Found " + docTimestamps.size() + " documents with timestamp values");

        if (docTimestamps.isEmpty()) {
            return;
        }

        // Sort by document ID to see physical order
        List<DocTimestamp> byDocId = new ArrayList<>(docTimestamps);
        Collections.sort(byDocId, Comparator.comparingInt(DocTimestamp::getDocId));

        // Sort by timestamp descending to see logical order
        List<DocTimestamp> byTimestamp = new ArrayList<>(docTimestamps);
        Collections.sort(byTimestamp, Comparator.comparingLong(DocTimestamp::getTimestamp).reversed());

        // Print the first 100 documents by document ID
        System.out.println("\nFirst 100 documents by document ID (physical order):");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        int limit = Math.min(100, byDocId.size());
        for (int i = 0; i < limit; i++) {
            DocTimestamp dt = byDocId.get(i);
            System.out.printf("DocID: %d, Timestamp: %d (%s)%n",
                    dt.getDocId(),
                    dt.getTimestamp(),
                    sdf.format(new Date(dt.getTimestamp())));
        }
    }

    // Helper class to store document ID and timestamp together
    private static class DocTimestamp {
        private final int docId;
        private final long timestamp;

        public DocTimestamp(int docId, long timestamp) {
            this.docId = docId;
            this.timestamp = timestamp;
        }

        public int getDocId() {
            return docId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private static void listAvailableFields(LeafReaderContext leaf) throws IOException {
        System.out.println("Available fields in segment:");
        FieldInfos fieldInfos = leaf.reader().getFieldInfos();
        for (int i = 0; i < fieldInfos.size(); i++) {
            FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
            String field = fieldInfo.name;

            // Check if it has point values
            boolean hasPointValues = fieldInfo.getPointDimensionCount() > 0;

            // Check if it has doc values
            boolean hasDocValues = fieldInfo.getDocValuesType() != DocValuesType.NONE;

            System.out.printf(" - %s (points: %s, docValues: %s)%n",
                    field, hasPointValues, hasDocValues);
        }
    }

    private static void listPointValueFields(LeafReaderContext leaf) throws IOException {
        System.out.println("Available point value fields in segment:");
        FieldInfos fieldInfos = leaf.reader().getFieldInfos();
        for (int i = 0; i < fieldInfos.size(); i++) {
            FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
            if (fieldInfo.getPointDimensionCount() > 0) {
                System.out.println(" - " + fieldInfo.name +
                        " (dims: " + fieldInfo.getPointDimensionCount() +
                        ", numBytes: " + fieldInfo.getPointNumBytes() + ")");
            }
        }
    }

    private static void listDocValueFields(LeafReaderContext leaf) throws IOException {
        System.out.println("Available doc value fields in segment:");
        FieldInfos fieldInfos = leaf.reader().getFieldInfos();
        for (int i = 0; i < fieldInfos.size(); i++) {
            FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                System.out.println(" - " + fieldInfo.name +
                        " (type: " + fieldInfo.getDocValuesType() + ")");
            }
        }
    }
}