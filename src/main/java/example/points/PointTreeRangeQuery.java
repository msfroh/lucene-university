// # How range queries over point trees work
//
// Previously, in VisualizePointTree, we output a binary K-D tree for a set of points in an IntField. In this example,
// we will once again write points for an IntField, but this will focus on how the tree structure makes efficient
// range matching possible.
//
// We will first run a regular PointRangeQuery using the high-level IndexSearcher API. Next, we will recreate the range
// query internals with a custom `PointValues.IntersectVisitor` and pass that to the `PointValues.intersect()` method.
// Finally, we'll implement our own tree "intersect" method to show how the tree and visitor work together to find
// documents that match the given range.
//
package example.points;

import example.basic.SimpleSearch;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class PointTreeRangeQuery {

    // ## The worked example
    public static void main(String[] args) throws IOException {
        // Create a temporary directory to hold the index, then create the Lucene Directory and IndexWriter.
        Path tmpDir = Files.createTempDirectory(SimpleSearch.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            // ### Index documents
            //
            // As in VisualizePointTree, we will generate 20,000 documents, distributed over 10,000 integer point values.
            //
            for (int i = 0; i < 10_000; i++) {
                writer.addDocument(List.of(new KeywordField("id", "first " + i, Field.Store.NO),
                        new IntField("val", i, Field.Store.NO)));
                writer.addDocument(List.of(new KeywordField("id", "second " + i, Field.Store.NO),
                        new IntField("val", i, Field.Store.NO)));
            }

            // ### Search over a range
            //
            // We will search over the range [2000,3999], which will match 4000 documents (since each point has two
            // associated documents).
            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Query pointRangeQuery = IntPoint.newRangeQuery("val", 2000, 3999);
                int count = searcher.search(pointRangeQuery, new TotalHitCountCollectorManager(searcher.getSlices()));
                System.out.println("Query " + pointRangeQuery + " matched " + count + " documents");

                // Let's do that again, but without the range query or index searcher. Instead, we will pass an
                // IntersectVisitor to collect the intersecting points' doc IDs.
                //
                // We'll allocate a single-element `int[]` to hold our document counter (so we can reference the
                // variable from our anonymous IntersectVisitor).
                //

                int[] countHolder = new int[1];
                PointValues.IntersectVisitor intersectVisitor = new PointValues.IntersectVisitor() {
                    // This version of `visit` gets called when we know that every doc in the current leaf node matches.
                    @Override
                    public void visit(int docID) throws IOException {
                        countHolder[0]++;
                    }

                    // This version of `visit` is called when the current leaf node partially matches the range.
                    @Override
                    public void visit(int docID, byte[] packedValue) throws IOException {
                        int val = IntPoint.decodeDimension(packedValue, 0);
                        if (val >= 2000 && val < 4000) {
                            countHolder[0]++;
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        int minVal = IntPoint.decodeDimension(minPackedValue, 0);
                        int maxVal = IntPoint.decodeDimension(maxPackedValue, 0);
                        if (minVal >= 2000 && maxVal <= 4000) {
                            return PointValues.Relation.CELL_INSIDE_QUERY;
                        } else if (minVal >= 4000 || maxVal <= 2000) {
                            return PointValues.Relation.CELL_OUTSIDE_QUERY;
                        }
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                };
                for (LeafReaderContext lrc : reader.leaves()) {
                    lrc.reader().getPointValues("val").intersect(intersectVisitor);
                }
                System.out.println("Our IntersectVisitor counted " + countHolder[0] + " documents");


                // Now we're going to use custom traversal code that roughly simulates the behavior of the `intersect`
                // method. See the `customIntersect` definition below.
                //
                // Reset the document count.
                countHolder[0] = 0;
                for (LeafReaderContext lrc : reader.leaves()) {
                    PointValues.PointTree tree = lrc.reader().getPointValues("val").getPointTree();
                    customIntersect(tree, intersectVisitor);
                }
                System.out.println("Our custom intersection logic counted " + countHolder[0] + " documents");
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
        }
    }

    private static void customIntersect(PointValues.PointTree tree, PointValues.IntersectVisitor visitor) throws IOException {
        switch (visitor.compare(tree.getMinPackedValue(), tree.getMaxPackedValue())) {
            case CELL_OUTSIDE_QUERY -> {
                // Nothing to match on this node. Return without matching anything.
            }
            case CELL_INSIDE_QUERY -> {
                // This node is entirely contained within the visitor's range. Collect everything under this node.
                collectEverything(tree, visitor);
            }
            case CELL_CROSSES_QUERY -> {
                // The node overlaps the visitor's range. If it's a leaf, then iterate through each point.
                // Otherwise, recurse into the children.
                if (tree.moveToChild()) {
                    // Not a leaf node. Visit the first child and all of its siblings.
                    customIntersect(tree, visitor);
                    while (tree.moveToSibling()) {
                        customIntersect(tree, visitor);
                    }
                    tree.moveToParent();
                } else {
                    // A leaf node. Consider each value in the node.
                    // Note that we don't need to visit siblings, since they're handled by the parent call.
                    tree.visitDocValues(visitor);
                }
            }
        }
    }

    // The following helper method will collect all documents under the current node (regardless of the documents
    // associated values). We call this when we have determined that the current node is entirely contained within
    // the query range.
    private static void collectEverything(PointValues.PointTree tree, PointValues.IntersectVisitor visitor) throws IOException {
        if (tree.moveToChild()) {
            // Not a leaf. Recursively call this method on the first child and its siblings before moving back up.
            collectEverything(tree, visitor);
            while (tree.moveToSibling()) {
                collectEverything(tree, visitor);
            }
            tree.moveToParent();
        } else {
            // This is a leaf. Collect all of its doc IDs.
            tree.visitDocIDs(visitor);
        }
    }
}
