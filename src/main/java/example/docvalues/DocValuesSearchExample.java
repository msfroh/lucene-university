package example.docvalues;

import example.basic.SimpleSearch;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DocValuesSearchExample {

    // ## What are doc values?
    // 
    // Doc values are Apache Lucene's column-stride field value storage, that could be used to store, per document: 
    // - numerics (single- or multi-valued)
    // - sorted keywords (single or multi-valued) 
    // - binary data blobs
    // 
    // Doc values are the on-disk data structure, built at document index time. They store the same values as the 
    // _source but in a column-oriented fashion that is way more  efficient for sorting and aggregations and 
    // are quite fast to access at search time (only the  value for field in question needs to be decoded per hit).
    // This is in contrast to Lucene's stored document fields,  which store all field values for one document together 
    // in a row-stride fashion, and are therefore relatively slow to access.
    // 
    // [1] https://www.elastic.co/guide/en/elasticsearch/reference/7.10/doc-values.html#doc-values
    // [2] https://www.elastic.co/blog/sparse-versus-dense-document-values-with-apache-lucene
    //
    // ## Creating Documents
    //
    // We will create a list of documents, where each document has a single numeric field `order` of type INT.
    //
    // We use the `SortedNumericDocValuesField` type to indicate that it's a numeric field with doc values that
    // will be used for sorting.
    //
    private static List<List<IndexableField>> createDocuments() {
        List<List<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            doc.add(new IntPoint("order", i));
            doc.add(new SortedNumericDocValuesField("order", i));
            docs.add(doc);
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
                // A field that both indexed {@link IntPoint}s and {@link SortedNumericDocValuesField}s with the same values
                final Query pointQuery = IntPoint.newRangeQuery("order", 5, 18);
                final Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery("order", 5, 18);
                // A `IndexOrDocValuesQuery` is a query that uses either an index structure (points or terms) or doc values 
                // in order to run a query, depending which one is more efficient.
                IndexOrDocValuesQuery indexOrDocValuesQuery = new IndexOrDocValuesQuery(pointQuery, dvQuery);
                // We ask `searcher` to run our `indexOrDocValuesQuery` and return the top 10 documents, sorted by `order`
                // field in reverse order.
                TopDocs topDocs = searcher.search(indexOrDocValuesQuery, 10, new Sort(new SortedNumericSortField("order", Type.INT, true)));
                // If our query had matched more than 10 documents, then `topDocs` would contain the top 10 documents,
                // while `topDocs.totalHits` would have the total number of matching documents (or a lower bound on the
                // total number of matching documents, if more than 1000 documents match).
                System.out.println("Query " + indexOrDocValuesQuery + " matched " + topDocs.totalHits + " documents:");
                // The `topDocs` contains a list of `ScoreDoc`, which just have scores and Lucene-generated doc IDs.
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    // The access to doc values is possible through `LeafReader` and not individual document(s). The `DocValues` 
                    // class has a number of utility methods to access doc values. The `MultiDocValues` simplifies access to doc values
                    // a bit by using `IndexReader` directly, not individual  `LeafReader`s.
                    final SortedNumericDocValues sortedNumericDocValues = MultiDocValues.getSortedNumericValues(reader, "order");
                    // Extract the doc values for a specific document by advancing the iterator. The `LeafReader` may no contain this
                    // specific document so we would need to check another  `LeafReader` instead.
                    if (sortedNumericDocValues.advanceExact(scoreDoc.doc)) {
                        System.out.print("Doc values for doc [" + scoreDoc.doc + "]: ");
                        for (int count = 0; count < sortedNumericDocValues.docValueCount(); ++count) {
                            System.out.print(sortedNumericDocValues.nextValue());
                        }
                        System.out.println();
                    }
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
