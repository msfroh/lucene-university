// # Visualizing a point tree
//
// In this example, we will look at how Lucene indexes numeric fields using "points". These points are stored
// in a structure called a binary K-d tree (see https://en.wikipedia.org/wiki/K-d_tree for more details). The  K-d tree
// makes range queries more efficient, as we are able to descend into the tree to find and visit the set of subtrees
// that share some overlap with the given range, while ignoring the rest of the tree.
//
// After indexing a few thousand documents with an integer field, we will call a helper function that prints the point
// tree for the integer field, including the number of documents associated with each node.
//
// We also suggest a couple of variations on the exercise:
// 1. Split the documents across multiple segments by flushing a few times during the indexing step.
// 2. Write the same point value for every document to see how that impacts the tree shape.
//
// Output for the original example and the variations can be found at the bottom of the file.
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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;


public class VisualizePointTree {


    // ## Helper function to print the `PointTree`
    //
    // We will use this recursive helper function to print the current node, then recursively visit the node's children
    // and siblings.
    //
    private static void printTree(PointValues.PointTree pointTree, int level) throws IOException {

        // For each node, we'll output the current level (indenting more as we go deeper), the min and max point
        // values for the node, and the number of documents under the node.
        //
        String indent = " ".repeat(level);
        System.out.println(indent + level + indent +
                " [" + IntPoint.decodeDimension(pointTree.getMinPackedValue(), 0) + "," +
                IntPoint.decodeDimension(pointTree.getMaxPackedValue(), 0) + "] - " +
                pointTree.size());
        // Next we recurse into the first child, if present. The recursive call will also visit the siblings (see a few
        // lines below). After visiting the child we need to return to this node before visiting our own siblings.
        //
        if (pointTree.moveToChild()) {
            printTree(pointTree, level + 1);
            pointTree.moveToParent();
        }
        // Visit the current node's siblings.
        //
        while (pointTree.moveToSibling()) {
            printTree(pointTree, level);
        }
    }

    // ## The worked example
    //
    public static void main(String[] args) throws IOException {
        // Create a temporary directory to hold the index, then create the Lucene Directory and IndexWriter.
        Path tmpDir = Files.createTempDirectory(VisualizePointTree.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {

            // ### Index documents
            //
            // We will generate 20,000 documents, distributed over 10,000 integer point values.
            //
            // For the `val` field, we use type `IntField`. This is a convenient field type that tells Lucene to store
            // the field as a doc value and as a point. Note that this field **does not** get indexed to terms.
            //
            // *Optional exercise:* Try modifying these to output the same `val` value for every document.
            for (int i = 0; i < 10_000; i++) {
                writer.addDocument(List.of(new KeywordField("id", "first " + i, Field.Store.NO),
                        new IntField("val", i, Field.Store.NO)));
                writer.addDocument(List.of(new KeywordField("id", "second " + i, Field.Store.NO),
                        new IntField("val", i, Field.Store.NO)));
                //
                // *Optional exercise:* Uncomment this flush block if you want to write the documents to 5 segments:
                //
                /*
                if ((i + 1) % 2000 == 0) {
                    writer.flush();
                }
                */
            }
            // ### Reading the point tree
            //
            // Now we open a reader from the writer and iterate over the segments. (If the `flush` block above was left
            // commented out, we have a single segment with all points. Otherwise, each segment has its own points.)
            //
            try (IndexReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext lrc : reader.leaves()) {
                    LeafReader lr = lrc.reader();
                    System.out.println("Tree for segment " + lrc.ord);
                    PointValues.PointTree pointTree = lr.getPointValues("val").getPointTree();
                    printTree(pointTree, 0);
                }
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
        }
    }


}
// ## Program output
//
/*
// This is the output of the program with a single segment. There are 20,000 documents across 10,000 distinct point
// values (0 to 9999).
//
// Note that the leaf nodes all correspond to blocks of (up to) 512 documents.

Tree for segment 0
0 [0,9999] - 20000
 1  [0,6144] - 12288
  2   [0,4096] - 8192
   3    [0,2048] - 4096
    4     [0,1024] - 2048
     5      [0,512] - 1024
      6       [0,256] - 512
      6       [256,512] - 512
     5      [512,1024] - 1024
      6       [512,768] - 512
      6       [768,1024] - 512
    4     [1024,2048] - 2048
     5      [1024,1536] - 1024
      6       [1024,1280] - 512
      6       [1280,1536] - 512
     5      [1536,2048] - 1024
      6       [1536,1792] - 512
      6       [1792,2048] - 512
   3    [2048,4096] - 4096
    4     [2048,3072] - 2048
     5      [2048,2560] - 1024
      6       [2048,2304] - 512
      6       [2304,2560] - 512
     5      [2560,3072] - 1024
      6       [2560,2816] - 512
      6       [2816,3072] - 512
    4     [3072,4096] - 2048
     5      [3072,3584] - 1024
      6       [3072,3328] - 512
      6       [3328,3584] - 512
     5      [3584,4096] - 1024
      6       [3584,3840] - 512
      6       [3840,4096] - 512
  2   [4096,6144] - 4096
   3    [4096,5120] - 2048
    4     [4096,4608] - 1024
     5      [4096,4352] - 512
     5      [4352,4608] - 512
    4     [4608,5120] - 1024
     5      [4608,4864] - 512
     5      [4864,5120] - 512
   3    [5120,6144] - 2048
    4     [5120,5632] - 1024
     5      [5120,5376] - 512
     5      [5376,5632] - 512
    4     [5632,6144] - 1024
     5      [5632,5888] - 512
     5      [5888,6144] - 512
 1  [6144,9999] - 7712
  2   [6144,8192] - 4096
   3    [6144,7168] - 2048
    4     [6144,6656] - 1024
     5      [6144,6400] - 512
     5      [6400,6656] - 512
    4     [6656,7168] - 1024
     5      [6656,6912] - 512
     5      [6912,7168] - 512
   3    [7168,8192] - 2048
    4     [7168,7680] - 1024
     5      [7168,7424] - 512
     5      [7424,7680] - 512
    4     [7680,8192] - 1024
     5      [7680,7936] - 512
     5      [7936,8192] - 512
  2   [8192,9999] - 3616
   3    [8192,9216] - 2048
    4     [8192,8704] - 1024
     5      [8192,8448] - 512
     5      [8448,8704] - 512
    4     [8704,9216] - 1024
     5      [8704,8960] - 512
     5      [8960,9216] - 512
   3    [9216,9999] - 1568
    4     [9216,9728] - 1024
     5      [9216,9472] - 512
     5      [9472,9728] - 512
    4     [9728,9999] - 544
     5      [9728,9984] - 512
     5      [9984,9999] - 32


// This is the output with multiple segments (i.e. flush block uncommented). Each segment has 4000 documents distributed
// across 2000 point values.

Tree for segment 0
0 [0,1999] - 4000
 1  [0,1024] - 2048
  2   [0,512] - 1024
   3    [0,256] - 512
   3    [256,512] - 512
  2   [512,1024] - 1024
   3    [512,768] - 512
   3    [768,1024] - 512
 1  [1024,1999] - 1952
  2   [1024,1536] - 1024
   3    [1024,1280] - 512
   3    [1280,1536] - 512
  2   [1536,1999] - 928
   3    [1536,1792] - 512
   3    [1792,1999] - 416
Tree for segment 1
0 [2000,3999] - 4000
 1  [2000,3024] - 2048
  2   [2000,2512] - 1024
   3    [2000,2256] - 512
   3    [2256,2512] - 512
  2   [2512,3024] - 1024
   3    [2512,2768] - 512
   3    [2768,3024] - 512
 1  [3024,3999] - 1952
  2   [3024,3536] - 1024
   3    [3024,3280] - 512
   3    [3280,3536] - 512
  2   [3536,3999] - 928
   3    [3536,3792] - 512
   3    [3792,3999] - 416
Tree for segment 2
0 [4000,5999] - 4000
 1  [4000,5024] - 2048
  2   [4000,4512] - 1024
   3    [4000,4256] - 512
   3    [4256,4512] - 512
  2   [4512,5024] - 1024
   3    [4512,4768] - 512
   3    [4768,5024] - 512
 1  [5024,5999] - 1952
  2   [5024,5536] - 1024
   3    [5024,5280] - 512
   3    [5280,5536] - 512
  2   [5536,5999] - 928
   3    [5536,5792] - 512
   3    [5792,5999] - 416
Tree for segment 3
0 [6000,7999] - 4000
 1  [6000,7024] - 2048
  2   [6000,6512] - 1024
   3    [6000,6256] - 512
   3    [6256,6512] - 512
  2   [6512,7024] - 1024
   3    [6512,6768] - 512
   3    [6768,7024] - 512
 1  [7024,7999] - 1952
  2   [7024,7536] - 1024
   3    [7024,7280] - 512
   3    [7280,7536] - 512
  2   [7536,7999] - 928
   3    [7536,7792] - 512
   3    [7792,7999] - 416
Tree for segment 4
0 [8000,9999] - 4000
 1  [8000,9024] - 2048
  2   [8000,8512] - 1024
   3    [8000,8256] - 512
   3    [8256,8512] - 512
  2   [8512,9024] - 1024
   3    [8512,8768] - 512
   3    [8768,9024] - 512
 1  [9024,9999] - 1952
  2   [9024,9536] - 1024
   3    [9024,9280] - 512
   3    [9280,9536] - 512
  2   [9536,9999] - 928
   3    [9536,9792] - 512
   3    [9792,9999] - 416



// Finally, here is the output if we modify the code to set `val` to 1 for every document. Notice that we still create
// the full binary tree structure with leaves of up to 512 documents, even though every node has the same bounds.
//

Tree for segment 0
0 [1,1] - 20000
 1  [1,1] - 12288
  2   [1,1] - 8192
   3    [1,1] - 4096
    4     [1,1] - 2048
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
    4     [1,1] - 2048
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
   3    [1,1] - 4096
    4     [1,1] - 2048
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
    4     [1,1] - 2048
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
     5      [1,1] - 1024
      6       [1,1] - 512
      6       [1,1] - 512
  2   [1,1] - 4096
   3    [1,1] - 2048
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
   3    [1,1] - 2048
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
 1  [1,1] - 7712
  2   [1,1] - 4096
   3    [1,1] - 2048
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
   3    [1,1] - 2048
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
  2   [1,1] - 3616
   3    [1,1] - 2048
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
   3    [1,1] - 1568
    4     [1,1] - 1024
     5      [1,1] - 512
     5      [1,1] - 512
    4     [1,1] - 544
     5      [1,1] - 512
     5      [1,1] - 32
 */

