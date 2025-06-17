package example.tools;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/*public class SingleChildBKDTreeExample {
    public static void main(String[] args) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);

        // Add multiple documents with the same point value
        for (int i = 0; i < 100; i++) {
            long val = i * 1000L;
            Document doc = new Document();
            doc.add(new LongPoint("point", val));
            doc.add(new NumericDocValuesField("point", val));
            writer.addDocument(doc);
        }

        // Add one outlier
        Document outlier = new Document();
        outlier.add(new LongPoint("point", 987654321L));
        outlier.add(new NumericDocValuesField("point", 987654321L));
        writer.addDocument(outlier);

        writer.commit();
        writer.forceMerge(1);
        writer.close();

        try (IndexReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                PointValues pointValues = leaf.reader().getPointValues("point");
                if (pointValues == null) {
                    System.out.println("No point values for field 'point'.");
                    return;
                }

                System.out.println("Total points: " + pointValues.size());
                System.out.println("Doc count: " + pointValues.getDocCount());

                PointValues.PointTree tree = pointValues.getPointTree();
                System.out.println("\nBKD Tree Traversal:");
                traverse(tree, 0);
            }
        }
    }

    private static void traverse(PointValues.PointTree tree, int depth) throws IOException {
        String indent = "  ".repeat(depth);

        byte[] min = tree.getMinPackedValue();
        byte[] max = tree.getMaxPackedValue();
        long minVal = LongPoint.decodeDimension(min, 0);
        long maxVal = LongPoint.decodeDimension(max, 0);

        System.out.printf("%sNode (size=%d) range=[%d, %d]%n", indent, tree.size(), minVal, maxVal);

        if (tree.moveToChild()) {
            int children = 0;
            do {
                children++;
                traverse(tree.clone(), depth + 1);
            } while (tree.moveToSibling());

            System.out.printf("%sChildren count: %d%n", indent, children);
            if (children == 1) {
                System.err.printf("⚠️  Single child found at depth %d, range=[%d, %d]%n", depth, minVal, maxVal);
            }
            tree.moveToParent();
        }
    }
}*/

/*public class SingleChildBKDTreeExample {
    public static void main(String[] args) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);

        // Add 999 documents with the same point value (highly skewed)
        for (int i = 0; i < 100; i++) {
            long val = 1000L;
            Document doc = new Document();
            doc.add(new LongPoint("point", val));
            doc.add(new NumericDocValuesField("point", val));
            writer.addDocument(doc);
        }

        // Add one outlier
        Document outlier = new Document();
        outlier.add(new LongPoint("point", 987654321L));
        outlier.add(new NumericDocValuesField("point", 987654321L));
        writer.addDocument(outlier);

        writer.commit();
        writer.forceMerge(1);
        writer.close();

        try (IndexReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                PointValues pointValues = leaf.reader().getPointValues("point");
                if (pointValues == null) {
                    System.out.println("No point values for field 'point'.");
                    return;
                }

                System.out.println("Total points: " + pointValues.size());
                System.out.println("Doc count: " + pointValues.getDocCount());

                PointValues.PointTree tree = pointValues.getPointTree();
                System.out.println("\nBKD Tree Traversal:");
                traverse(tree, 0);
            }
        }
    }

    private static void traverse(PointValues.PointTree tree, int depth) throws IOException {
        String indent = "  ".repeat(depth);

        byte[] min = tree.getMinPackedValue();
        byte[] max = tree.getMaxPackedValue();
        long minVal = LongPoint.decodeDimension(min, 0);
        long maxVal = LongPoint.decodeDimension(max, 0);

        System.out.printf("%sNode (size=%d) range=[%d, %d]%n", indent, tree.size(), minVal, maxVal);

        if (tree.moveToChild()) {
            int children = 0;
            do {
                children++;
                traverse(tree.clone(), depth + 1);
            } while (tree.moveToSibling());
            System.out.println(indent + "Children count: " + children);
            tree.moveToParent();
        }
    }
}*/



public class SingleChildBKDTreeExample {
    public static void main(String[] args) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);

        // Add 100 documents with the same point value (highly skewed)
        for (int i = 0; i < 100; i++) {
            long val = 1000L;
            Document doc = new Document();
            doc.add(new LongPoint("point", val));
            doc.add(new NumericDocValuesField("point", val));
            writer.addDocument(doc);
        }

        // Add one outlier
        Document outlier = new Document();
        outlier.add(new LongPoint("point", 987654321L));
        outlier.add(new NumericDocValuesField("point", 987654321L));
        writer.addDocument(outlier);

        writer.commit();
        writer.forceMerge(1);
        writer.close();

        try (IndexReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                PointValues pointValues = leaf.reader().getPointValues("point");
                if (pointValues == null) {
                    System.out.println("No point values for field 'point'.");
                    return;
                }

                System.out.println("Total points: " + pointValues.size());
                System.out.println("Doc count: " + pointValues.getDocCount());

                PointValues.PointTree tree = pointValues.getPointTree();

                // Original traversal
                System.out.println("\nBKD Tree Traversal:");
                traverse(tree.clone(), 0);

                // Visual tree
                System.out.println("\nBKD Tree Visualization:");
                printTree(tree, "", true);  // Don't clone here
            }
        }
    }

    private static void traverse(PointValues.PointTree tree, int depth) throws IOException {
        String indent = "  ".repeat(depth);

        byte[] min = tree.getMinPackedValue();
        byte[] max = tree.getMaxPackedValue();
        long minVal = LongPoint.decodeDimension(min, 0);
        long maxVal = LongPoint.decodeDimension(max, 0);

        System.out.printf("%sNode (size=%d) range=[%d, %d]%n", indent, tree.size(), minVal, maxVal);

        if (tree.moveToChild()) {
            int children = 0;
            do {
                children++;
                traverse(tree.clone(), depth + 1);
            } while (tree.moveToSibling());
            System.out.println(indent + "Children count: " + children);
            tree.moveToParent();
        }
    }

    private static void printTree(PointValues.PointTree tree, String prefix, boolean isLast) throws IOException {
        byte[] min = tree.getMinPackedValue();
        byte[] max = tree.getMaxPackedValue();
        long minVal = LongPoint.decodeDimension(min, 0);
        long maxVal = LongPoint.decodeDimension(max, 0);

        // Check if this is a leaf
        PointValues.PointTree testTree = tree.clone();
        boolean isLeaf = !testTree.moveToChild();

        // Print current node
        String connector = prefix.isEmpty() ? "" : (isLast ? "└── " : "├── ");
        String nodeType = isLeaf ? "LEAF" : "INNER";
        System.out.printf("%s%s%s[%d pts, range: %d-%d]%n",
                prefix, connector, nodeType, tree.size(), minVal, maxVal);

        // If not a leaf, print all children
        if (!isLeaf) {
            String childPrefix = prefix + (prefix.isEmpty() ? "" : (isLast ? "    " : "│   "));

            // Move to first child
            tree.moveToChild();

            // Count siblings first
            int siblingCount = 1;
            PointValues.PointTree countTree = tree.clone();
            while (countTree.moveToSibling()) {
                siblingCount++;
            }

            // Now print each child
            int currentChild = 0;
            do {
                currentChild++;
                boolean isLastChild = (currentChild == siblingCount);
                printTree(tree.clone(), childPrefix, isLastChild);
            } while (tree.moveToSibling());
        }
    }
}

