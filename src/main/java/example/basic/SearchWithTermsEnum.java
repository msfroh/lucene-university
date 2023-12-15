// # Searching with a terms enum
//
// This example is essentially a repeat of SimpleSearch, but instead of using an `IndexSearcher`, we will access the
// underlying index components via `IndexReader` -- specifically, we'll:
//
// 1. Iterate through the index segments under the `IndexReader` (but there will only be one segment).
// 2. Load a `TermsEnum` for the `text` field.
// 3. Seek to the `fox` term in the enum.
// 4. Iterate through the matching documents via a `PostingsEnum`.
// 5. Explore some of the other attributes from the `PostingsEnum`.
//
// This is not intended as a "how-to" example, since you should use `IndexSearcher` for searching. Instead, it should
// help demystify some of what's happening under the `IndexSearcher`.
//
package example.basic;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SearchWithTermsEnum {

    // ## Creating Documents
    //
    // This set of documents is copied over from SimpleSearch. I'm just keeping it here too, because I want every
    // example class to be fully self-contained.
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

    // ## Example code
    //
    public static void main(String[] args) throws IOException {

        // As in the previous example, we will create a temporary directory, open it as a Lucene `Directory`, and
        // create an `IndexWriter`. Then we'll add each of the example documents.
        //
        Path tmpDir = Files.createTempDirectory(SearchWithTermsEnum.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (List<IndexableField> doc : createDocuments()) {
                writer.addDocument(doc);
                // *Exercise:* If you want to write each document to its own segment, you can uncomment this line:
                /* writer.flush(); */
            }
            // Open the index reader, but this time we won't use an `IndexSearcher`.
            try (IndexReader reader = DirectoryReader.open(writer)) {
                // The `IndexReader` provided by `DirectoryReader` is a form of `CompositeReader`, made up of
                // underlying "leaves". These leaves are the flushed write-once segments of the index.
                //
                // In this case, because we only flushed once, in the `DirectoryReader.open()` call, the index
                // only has one segment.
                //
                System.out.println("Our reader has " + reader.leaves().size() + " segment(s)");

                // We'll iterate through the list of segments, because that's essentially what `IndexSearcher` does,
                // though in this case it's a list of length 1.
                //
                for (LeafReaderContext leafReaderContext : reader.leaves()) {
                    // We load the terms for the "text" field. The way we tokenized the text field (using the
                    // default, implicit `StandardAnalyzer`), there is one term for each unique word in the documents.
                    //
                    // If we have multiple segments (if we uncommented the `writer.flush()` line above), we see that
                    // each segment has its own term dictionary.
                    //
                    Terms textTerms = Terms.getTerms(leafReaderContext.reader(), "text");
                    System.out.println("Segment " + leafReaderContext.ord + " has " + textTerms.size() + " terms");

                    // We can iterate through all the terms using a `TermsEnum`:
                    //
                    TermsEnum textTermsEnum = textTerms.iterator();
                    System.out.println("The terms are:");
                    BytesRef curTerm;
                    while ((curTerm = textTermsEnum.next()) != null) {
                        System.out.println(curTerm.utf8ToString());
                    }

                    // When searching, though, we don't iterate through all the terms, instead we use the `seekExact`
                    // method.
                    //
                    // Under the hood, all terms are indexed as arrays of bytes. Text terms are converted
                    // to bytes by UTF-8 encoding. We can use the `new BytesRef(CharSequence)` constructor to
                    // do the conversion.
                    //
                    textTermsEnum = textTerms.iterator(); /* Reset our iterator, since it previously hit the end */
                    if (textTermsEnum.seekExact(new BytesRef("fox"))) {
                        System.out.println("Found term 'fox' in segment " + leafReaderContext.ord);

                        // Once the `TermsEnum` is pointing at a specific term, we can efficiently (`O(1)` time) see how
                        // many documents contain the term.
                        //
                        System.out.println("The term 'fox' occurs in " + textTermsEnum.docFreq() + " documents in the current segment.");

                        // To find the specific documents matching a term, we retrieve a `PostingsEnum`. The `null`
                        // parameter could be an old `PostingsEnum` that we want to reuse to reduce object allocation
                        // overhead, but we don't have an old `PostingsEnum`.
                        //
                        PostingsEnum postingsEnum = textTermsEnum.postings(null);
                        int docId;
                        while ((docId = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            // The matching document IDs are local to the current segment. That is, each segment
                            // starts counting doc IDs again from 0.
                            //
                            System.out.println("Matching doc with id " + docId + ":");
                            // As with the SimpleSearch example, we can load the stored fields to retrieve the full
                            // `text` field value. In this case, though, we're loading the segment-local `StoredFields`.
                            StoredFields storedFields = leafReaderContext.reader().storedFields();
                            System.out.println(storedFields.document(docId).get("text"));
                        }
                    } else {
                        // This should only happen if we explicitly flush.
                        //
                        System.out.println("Did not find term 'fox' in segment " + leafReaderContext.ord);
                    }
                    //
                }
            }
        } finally {
            // ## Cleanup
            //
            // As in the previous example, we'll clean up the temporary files to avoid cluttering your hard disk.
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            Files.deleteIfExists(tmpDir);
        }

    }


}
