package example.advanced;

import example.basic.SimpleSearch;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * In this test we are going to copy most of the {@link example.basic.SimpleSearch} documents.
 * However, in this case we are going to leverage FunctionQuery to find an alternative source of scoring.
 * FunctionQuery is a special query that allows us to change how score is calculated from scratch by providing a source
 * of score and a function to modify it.
 * We will demonstrate that alternative score can come from either doc field or doc value field
 */
public class FunctionQuerySearchExample {

    // ## Creating Documents
    //
    // We will create a list of documents, where each document has a single field, `text`.
    //
    // We use the `TextField` type to indicate that it's a "full text" field, to be split into individual tokens
    // during indexing.
    //
    // In order to retrieve the original field value in our search results, we indicate that we want the field value
    // stored using `Field.Store.YES`.
    private static List<List<IndexableField>> createDocuments() {
        List<String> texts = List.of(
                "Lorem ipsum, dolor sit amet", // Score set to 0
                "She left the web, she left the loom, she made three paces through the room", // Score set to 1
                "The sly fox sneaks past the oblivious dog", // Score set to 2
                "The quick fox jumped over the lazy, brown dog" // Score set to 3
                );
        List<List<IndexableField>> docs = new ArrayList<>();
        int i = 0;
        for (String text : texts) {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new TextField("text", text, Field.Store.YES));
            doc.add(new FloatField("floatField", i, Field.Store.YES));
            doc.add(new FloatDocValuesField("floatDocValuesField", i));
            docs.add(doc);
            i++;
        }
        return docs;
    }

    // ## The example code
    //
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
                // A `TermQuery` is one of the simpler query types supported by Lucene. It is able to search for a single
                // "term" (usually a word, but not necessarily) in a field. In this case, we're going to search for the
                // term `fox` in the `text` field.
                TermQuery termQuery = new TermQuery(new Term("text", "fox"));

                // A `FunctionScoreQuery` is a query that can wrap around the `TermQuery` and provide an alternative source
                // for scoring using either a DocField or DocValuesField, in this example we will use the DocValuesField
                FunctionScoreQuery functionScoreQuery =
                        new FunctionScoreQuery(
                                termQuery,
                                DoubleValuesSource.fromFloatField("floatDocValuesField"));


                // We ask `searcher` to run our `termQuery` and return the top 10 documents. There are only 2 documents
                // in our index with the term `fox`, so it will only return those 2.
                TopDocs topDocs = searcher.search(functionScoreQuery, 10);
                // If our query had matched more than 10 documents, then `topDocs` would contain the top 10 documents,
                // while `topDocs.totalHits` would have the total number of matching documents (or a lower bound on the
                // total number of matching documents, if more than 1000 documents match).
                System.out.println("Query " + termQuery + " matched " + topDocs.totalHits + " documents:");
                // The `topDocs` contains a list of `ScoreDoc`, which just have scores and Lucene-generated doc IDs.
                // Since these doc IDs are likely meaningless to us as users, we ask the reader for a `StoredFields`
                // instance able to retrieve stored field values based on the doc IDs.
                StoredFields storedFields = reader.storedFields();
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    // Using the doc's ID, we load a `Document`, and load the `text` field (the only field).
                    String storedText = storedFields.document(scoreDoc.doc).get("text");
                    // Each document has a score based on it's value at `floatDocValuesField`.
                    // Note that here we got the results sorted based on this score as opposed to the BM25 score from the {@link SimpleSearch} example.
                    //
                    // In this case both of our matching documents contain the word `fox` once, so they're tied there.
                    // The last document is the one with the lower value in `floatDocValuesField` (e.g. score = 2.0)
                    // than the first document (score = 3.0) .
                    System.out.println(scoreDoc.score + " - " + scoreDoc.doc + " - " + storedText);
                    //
                }
            }
            //
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
        }
    }
}
