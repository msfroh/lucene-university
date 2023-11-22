// # Simple search example
//
// This will walk through a very simple search example with Lucene.
//
// The example has the following steps:
// 1. Create documents. Documents are iterable collections of `IndexableField`.
// 2. Open an `IndexWriter` over a filesystem directory (`FSDirectory`).
// 3. Write the documents using the `IndexWriter`.
// 4. Open an `IndexReader` from the `IndexWriter`, then open an `IndexSearcher` around that `IndexReader`.
// 5. Construct a `TermQuery` to search for the word `fox` in the `text` field.
// 6. Use the `IndexSearcher` to find the documents that match the `TermQuery`.
// 7. Iterate through the returned documents to output their score, ID, and stored `text` value.

package example.basic;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SimpleSearch {

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
                "The quick fox jumped over the lazy, brown dog",
                "Lorem ipsum, dolor sit amet",
                "She left the web, she left the loom, she made three paces through the room",
                "The sly fox sneaks past the oblivious dog"
        );
        List<List<IndexableField>> docs = new ArrayList<>();
        for (String text : texts) {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new TextField("text", text, Field.Store.YES));
            docs.add(doc);
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
                // We ask `searcher` to run our `termQuery` and return the top 10 documents. There are only 2 documents
                // in our index with the term `fox`, so it will only return those 2.
                TopDocs topDocs = searcher.search(termQuery, 10);
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
                    // Each document has a BM25 score based on their relevance to the query.
                    // See https://en.wikipedia.org/wiki/Okapi_BM25 for details on the BM25 formula.
                    //
                    // The parts of the formula relevant to this example are:
                    // 1. A search term occurring more frequently in a document makes the score go up.
                    // 2. More total terms in the field makes the score go down.
                    //
                    // In this case both of our matching documents contain the word `fox` once, so they're tied there.
                    // The last document is shorter than the first document, so the last document gets a higher score.
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
