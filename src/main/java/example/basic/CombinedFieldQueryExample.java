// # What is Lucene CombinedFieldQuery?
//
// Lucene's `CombinedFieldQuery` is a powerful query type that allows you to search across multiple fields as if they were
// a single field. It is particularly useful when you have multiple fields that contribute to the same conceptual aspect
// of a document, and you want to perform a search that considers all of them together.
//
// For example, if you have separate fields for the "title" and "description" of a document, you can use `CombinedFieldQuery`
// to treat them as a single searchable entity. Lucene will combine the term frequencies across all the fields when
// computing scores, providing a more holistic search experience.
//
// In this example, we are combining three fields (`year`, `make`, and `model`) and running a search for the string
// "2011 Toyota Corolla". Lucene will internally combine the term frequencies across these fields,
// and provide relevant results ranked by their scores.
//
// ## Key Features of CombinedFieldQuery:
// - Combines term frequencies across multiple fields.
// - Useful for handling searches where terms could appear in different fields (e.g., `title`, `description`).
// - Simplifies complex multi-field queries by treating fields as a unified text source.
// - Enhances relevance scoring by considering all specified fields in the context of the query.
//
// In the following example, we demonstrate how to use `CombinedFieldQuery` to perform a search over the fields `year`, `make`, and `model`.

package example.basic;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class CombinedFieldQueryExample {
    public static void main(String[] args) throws Exception {
        // # Step 1: Create a new index
        // In-memory ByteBuffersDirectory is used for indexing
        Directory directory = new ByteBuffersDirectory();

        // Create the configuration for the index writer, using a standard analyzer
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

        // Create an IndexWriter to add documents to the index
        IndexWriter indexWriter = new IndexWriter(directory, config);

        // # Step 2: Add documents to the index
        // Add sample documents with year (as a string), make, and model fields
        addDocument(indexWriter, "2011", "Toyota", "Corolla");
        addDocument(indexWriter, "2015", "Honda", "Civic");
        addDocument(indexWriter, "2020", "Toyota", "Camry");

        // Close the IndexWriter after adding all documents
        indexWriter.close();

        // # Step 3: Search the index
        // Open a DirectoryReader to read the index
        DirectoryReader reader = DirectoryReader.open(directory);

        // Create an IndexSearcher to perform searches on the indexed data
        IndexSearcher searcher = new IndexSearcher(reader);

        // # Step 4: Build the CombinedFieldQuery
        // Create a CombinedFieldQuery builder that allows us to query across multiple fields
        CombinedFieldQuery.Builder queryBuilder = new CombinedFieldQuery.Builder();

        // Add the fields we want to query: year, make, and model
        queryBuilder.addField("year");
        queryBuilder.addField("make");
        queryBuilder.addField("model");

        // Add terms for the specific query (e.g., "2011 Toyota Corolla")
        queryBuilder.addTerm(new BytesRef("2011")); // Query for the year as a string
        queryBuilder.addTerm(new BytesRef("Toyota")); // Query for the make
        queryBuilder.addTerm(new BytesRef("Corolla")); // Query for the model

        // Build the query from the queryBuilder
        Query query = queryBuilder.build();

        // # Step 5: Execute the search
        // Perform the search using the IndexSearcher and retrieve the top 10 matching documents
        TopDocs topDocs = searcher.search(query, 10);

        // Iterate over the search results and print the relevant fields
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            // Access the stored fields of the document using the storedFields() method
            Document doc = searcher.storedFields().document(scoreDoc.doc);

            // Retrieve the year as a string
            String yearValue = doc.get("year");

            // Print the search result details
            // Output: Score: 0.44583148, Year: 2011, Make: Toyota, Model: Corolla
            System.out.println("Score: " + scoreDoc.score +
                    ", Year: " + (yearValue != null ? yearValue : "N/A") +
                    ", Make: " + doc.get("make") +
                    ", Model: " + doc.get("model"));
        }

        // Close the DirectoryReader after reading the search results
        reader.close();
    }

    // # Step 6: Adding documents to the index
    // Helper method to add a document to the index
    private static void addDocument(IndexWriter indexWriter, String year, String make, String model) throws IOException {
        // Create a new document
        Document doc = new Document();

        // Add the year field as a string (stored)
        doc.add(new TextField("year", year, Field.Store.YES));

        // Add the make field as a text field (stored)
        doc.add(new TextField("make", make, Field.Store.YES));

        // Add the model field as a text field (stored)
        doc.add(new TextField("model", model, Field.Store.YES));

        // Add the document to the index
        indexWriter.addDocument(doc);
    }
}
