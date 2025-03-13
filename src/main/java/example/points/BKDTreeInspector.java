package example.points;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class BKDTreeInspector {

    public static void main(String[] args) throws Exception {
        // Adjust the path below to point to your index directory.
        Path indexPath = Paths.get("/home/ec2-user/OS_30/OS_30_1_968eafbd37ef0b864e887643c74291cc3e5ca0d0/nodes/0/indices/rOA5QvdyQ-2JYf3h0WZmFw/0/index/");
        try (FSDirectory directory = FSDirectory.open(indexPath);
             IndexReader reader = DirectoryReader.open(directory)) {

            IndexSearcher searcher = new IndexSearcher(reader);

            // Run a range query over the "val" field in the range [2000,3999].
            Query query = IntPoint.newRangeQuery("val", 2000, 3999);
            int count = searcher.search(query, new TotalHitCountCollectorManager(searcher.getSlices()));
            System.out.println("Range query " + query + " matched " + count + " documents");

            // Now traverse the BKD tree for field "val" in each leaf (or partition)
            for (LeafReaderContext leaf : reader.leaves()) {
                PointValues pv = leaf.reader().getPointValues("val");
                if (pv != null) {
                    System.out.println("\n--- BKD tree for leaf: " + leaf + " ---");
                    PointValues.PointTree tree = pv.getPointTree();
                    dumpBKD(tree, 0);
                } else {
                    System.out.println("Leaf " + leaf + " has no point values for field 'val'.");
                }
            }
        }
    }

    /**
     * Recursively dumps the BKD tree structure.
     *
     * @param tree  The current BKD tree node.
     * @param level The current depth (for indentation).
     */
    private static void dumpBKD(PointValues.PointTree tree, int level) throws IOException {
        String indent = " ".repeat(level * 2);
        byte[] minPackedValue = tree.getMinPackedValue();
        byte[] maxPackedValue = tree.getMaxPackedValue();
        System.out.println(indent + "Node at level " + level);
        System.out.println(indent + "  Min value: " + decodeValue(minPackedValue));
        System.out.println(indent + "  Max value: " + decodeValue(maxPackedValue));

        // If we can move to a child, this node is internal.
        if (tree.moveToChild()) {
            do {
                dumpBKD(tree, level + 1);
            } while (tree.moveToSibling());
            tree.moveToParent();
        } else {
            // Leaf node: iterate over each document in this leaf.
            System.out.println(indent + "  Leaf node docs:");
            tree.visitDocValues(new PointValues.IntersectVisitor() {
                @Override
                public void visit(int docID, byte[] packedValue) throws IOException {
                    int val = IntPoint.decodeDimension(packedValue, 0);
                    System.out.println("Leaf docID: " + docID + ", value: " + val);
                }

                @Override
                public void visit(int docID) throws IOException {
                    // Called when an entire cell is inside the query.
                    System.out.println("Leaf docID: " + docID + " (full cell)");
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    // For our dumping purpose, we simply return CELL_INSIDE_QUERY to ensure we visit every doc.
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            });

        }
    }

    /**
     * Decodes a single-dimensional int value from a packed value.
     *
     * @param packedValue The packed byte array.
     * @return The decoded integer.
     */
    private static int decodeValue(byte[] packedValue) {
        return IntPoint.decodeDimension(packedValue, 0);
    }
}
