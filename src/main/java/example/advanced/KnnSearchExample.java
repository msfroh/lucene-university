package example.advanced;

import example.basic.SimpleSearch;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class KnnSearchExample {

    // ## Creating Documents
    //
    // We will create a list of documents, where each document where each document has two fields, an 'id' field of type string
    // and a 'knnFloatField' of type KnnFloatVectorField.
    //
    // The 'knnFloatField' is essentially a float array representing a vector
    //
    // In order to retrieve the original field value in our search results, we indicate that we want the 'id' field value
    // stored using `Field.Store.YES`.
    private static List<List<IndexableField>> createDocuments() {
        List<float[]> vectors = List.of(
                new float[] {1, 2, 3}, // id 0
                new float[] {4, 5, 6}, // id 1
                new float[] {7, 8, 9}, // id 2
                new float[] {10, 11, 12} // id 3
        );

        List<List<IndexableField>> docs = new ArrayList<>();
        int i = 0;
        for (float[] vector : vectors) {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            // we are going to use Euclidean distance for our vector fields
            doc.add(new KnnFloatVectorField("knnFloatField", vector, VectorSimilarityFunction.EUCLIDEAN));
            docs.add(doc);
            i++;
        }

        return docs;
    }

    public static void main(String[] args) throws IOException {
        // We start by creating a temporary directory, wherever your JVM specifies its default `java.io.tmpdir`.
        // This will hold the index files.
        Path tmpDir = Files.createTempDirectory(SimpleSearch.class.getSimpleName());

        // We "open" the file system directory as a Lucene `Directory`, then open an `IndexWriter` able to write to
        // that directory.
        //
        // Lucene places and locks a `write.lock` file in the directory to make sure that other processes are not able
        // to write to the directory while we hold the lock (as long as those other processes try to obtain the lock).
        try (Directory directory = FSDirectory.open(tmpDir);
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            // Use our `createDocuments` helper function to create the documents and write them to the index.
            // Since they are all written at once without calling `writer.flush()` in between, they get written
            // contiguously in a single segment. We'll cover segments in another lesson.
            for (List<IndexableField> doc : createDocuments()) {
                writer.addDocument(doc);
            }

            // Open an `IndexReader` based on the `writer`. This causes `writer` to flush pending documents, so they
            // can be read. Note that `reader` has a view of the index at this point in time.
            // If we were to write more documents with `writer`, they would not be visible to `reader`.
            try (IndexReader reader = DirectoryReader.open(writer)) {
                // An `IndexReader` is able to read the underlying structure of the index, but high-level searching
                // requires an `IndexSearcher`. We'll explore the low-level `IndexReader` operations in a later lesson.
                IndexSearcher searcher = new IndexSearcher(reader);
                // A `KnnFloatVectorQuery` is the most basic KNN query supported by Lucene. Given a query vector, it is able to
                // retrieve the K closest neighbours. In this case, we're going to search for the
                // vector [1, 2, 3] in the `knnFloatField` field and search for the 3 (k = 3) nearest neighbours.
                // Meaning we are going to search for the 3 "closest" vectors.
                // The calculation of similarity between vectors in the `knnFloatField` will be based on Euclidean distance
                // because that's how we declared the similarity function for the field.
                // However, remember that for scoring purposes we want the most similar/matching documents to have the higher score
                // while Euclidean distance is _lower_ for more "similar" vectors...
                // Therefore, the score will be the _inverse_ of the Euclidean distance with the formula score = 1 / (1 + distance(v1, v2)).
                KnnFloatVectorQuery knnFloatVectorQuery = new KnnFloatVectorQuery("knnFloatField", new float[] {1, 2, 3}, 3);
                // We ask `searcher` to run our `knnFloatVectorQuery` and return the top 10 documents.
                // Since vector distance can be measured to all 4 documents we created that contained vector field,
                // we would expect to get 4 results sorted from the closest to the furthest.
                // However, we already defined the `knnFloatVectorQuery` to only return k = 3, meaning that we will only
                // remain with 3 results sorted from nearest to furthest.
                // In our case we will expect the vector [1, 2, 3] to be the "closest" result because
                // it's exactly similar to the vector we are searching for and therefore the Euclidean distance between them is 0
                // and the score would be score = 1 / (1 + 0)
                TopDocs topDocs = searcher.search(knnFloatVectorQuery, 10);
                // If our query had matched more than 10 documents, then `topDocs` would contain the top 10 documents,
                // while `topDocs.totalHits` would have the total number of matching documents (or a lower bound on the
                // total number of matching documents, if more than 1000 documents match).
                System.out.println("Query " + knnFloatVectorQuery + " got " + topDocs.totalHits + ":");
                // The `topDocs` contains a list of `ScoreDoc`, which just have scores and Lucene-generated doc IDs.
                // Since these doc IDs are likely meaningless to us as users, we ask the reader for a `StoredFields`
                // instance able to retrieve stored field values based on the doc IDs.
                StoredFields storedFields = reader.storedFields();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    // Using the doc's ID, we load a `Document`, and load the `id` `String` field (the field that defines global UUID).
                    final String globalId = storedFields.document(scoreDoc.doc).get("id");
                    // Each document has a score which is inverse to the Euclidean distance with a theoretical Min of slightly more than 0
                    // and a Max of 1 (see our example when the vectors are identical).
                    // See https://en.wikipedia.org/wiki/Euclidean_distance for details on the Euclidean distance formula.
                    System.out.println(scoreDoc.score + " - " + scoreDoc.doc + " - " + globalId);
                    //
                }
            }
        } finally {
            //
            // ## Clean up
            //
            // Before we finish the program, we delete each of the files in the directory.
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
            System.out.println("cleanup completed");
        }
    }
}
