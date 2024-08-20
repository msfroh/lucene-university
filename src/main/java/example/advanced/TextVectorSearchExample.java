// # Text Vector Search Example
//
// This example demonstrates how to use Lucene to index documents with textual content
// and perform a `KNN` (k-nearest neighbors) search using vector representations of the text.
//
// The example includes the following steps:
// 1. Create and index documents with textual content and vector fields.
// 2. Convert text into vectors (using a hypothetical method).
// 3. Perform a `KNN` search using a query vector.
// 4. Retrieve and display the top matching documents based on the `KNN` search.

package example.advanced;

import org.apache.lucene.document.Document;  // For creating and manipulating Lucene documents
import org.apache.lucene.document.Field;  // Base class for all fields in a Lucene document
import org.apache.lucene.document.TextField;  // For indexing and storing textual content
import org.apache.lucene.document.KnnFloatVectorField;  // For storing and indexing float vectors for `KNN` searches
import org.apache.lucene.index.DirectoryReader;  // For reading an index
import org.apache.lucene.index.IndexWriter;  // For writing documents to an index
import org.apache.lucene.index.IndexWriterConfig;  // For configuring the IndexWriter
import org.apache.lucene.search.IndexSearcher;  // For searching an index
import org.apache.lucene.search.KnnFloatVectorQuery;  // For querying using float vectors
import org.apache.lucene.search.TopDocs;  // For holding search results
import org.apache.lucene.store.Directory;  // Base class for storage implementations
import org.apache.lucene.store.ByteBuffersDirectory;  // In-memory implementation of Directory for testing

public class TextVectorSearchExample {

    // ## The main method
    //
    // This is the entry point of the example. It will perform all steps necessary to:
    // 1. Create an in-memory index.
    // 2. Index example documents.
    // 3. Perform a `KNN` search on the indexed data.
    // 4. Display the search results.
    public static void main(String[] args) throws Exception {
        // Create an in-memory index using `ByteBuffersDirectory`
        Directory directory = new ByteBuffersDirectory();  // Creates an in-memory directory to store the index

        // Configure the `IndexWriter`
        IndexWriterConfig config = new IndexWriterConfig();  // Configures the `IndexWriter` with default settings
        IndexWriter indexWriter = new IndexWriter(directory, config);  // Creates an `IndexWriter` to add documents to the in-memory index

        // ## Indexing Example Documents
        //
        // We will index a set of example documents. Each document contains text that will be converted to a vector
        // for the purpose of `KNN` search.
        String[] texts = {
                "Lucene is a powerful search library.",  // Document 1: Text content about Lucene
                "OpenSearch is built on top of Lucene.",  // Document 2: Text content about OpenSearch
                "Text search is crucial in modern applications."  // Document 3: Text content about the importance of text search
        };

        // Index the documents
        for (String text : texts) {
            Document doc = createDocument(text);  // Creates a Lucene Document for each text
            indexWriter.addDocument(doc);  // Adds the document to the index
        }

        indexWriter.close();  // Closes the `IndexWriter` to finalize the index and release resources

        // ## Perform a Vector Search
        //
        // We will now perform a `KNN` search using a query vector. The vector is generated from a query text.
        String queryText = "search engine library";  // Defines a query text for searching
        float[] queryVector = textToVector(queryText);  // Converts the query text to a vector (dummy vector in this case)

        DirectoryReader reader = DirectoryReader.open(directory);  // Opens a `DirectoryReader` to read the index

        // Create a `KNN` query with the vector field
        KnnFloatVectorQuery query = new KnnFloatVectorQuery("vector", queryVector, 2);  // Creates a `KNN` query to find the nearest vectors

        // Create an `IndexSearcher` to perform the search
        IndexSearcher searcher = new IndexSearcher(reader);  // Creates an `IndexSearcher` to perform the search
        TopDocs topDocs = searcher.search(query, 10);  // Executes the query and retrieves the top 10 results

        // ## Display the Search Results
        //
        // The search results will be displayed, including the text of each matching document and its relevance score.
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            // Retrieves the document by ID from the search results
            Document doc = searcher.storedFields().document(topDocs.scoreDocs[i].doc);
            // Prints the document text and its score
            System.out.println("Found doc: " + doc.get("text") + " with score: " + topDocs.scoreDocs[i].score);
        }

        reader.close();  // Closes the `DirectoryReader` to release resources
        directory.close();  // Closes the in-memory directory to release resources
    }

    // ## Helper method to create a Lucene Document
    //
    // This method creates a Lucene Document from the provided text. It converts the text into a vector
    // and adds both the text and the vector as fields in the document.
    private static Document createDocument(String text) {
        // Convert text to vector (using a hypothetical method)
        float[] vector = textToVector(text);  // Converts the document text to a vector

        Document doc = new Document();  // Creates a new Lucene Document
        doc.add(new TextField("text", text, Field.Store.YES));  // Adds the text field to the document, storing the original text
        doc.add(new KnnFloatVectorField("vector", vector));  // Adds the vector field for KNN search

        return doc;  // Returns the created document
    }

    // ## Hypothetical method to convert text to a vector
    //
    // This method is a placeholder for converting text into a vector representation. In a real application,
    // this would likely use a model like `BERT` or `Word2Vec` to generate the vector.
    private static float[] textToVector(String text) {
        // This method would typically use a model like `BERT` or `Word2Vec`
        // Here, we return a dummy vector for demonstration
        float[] vector = new float[768];  // Creates a dummy vector of size 768
        // Normally, you'd populate this vector with actual values from a model.
            // Example: `BertModel model = BertModel.load("bert-base-uncased")`;
        return vector;  // Returns the dummy vector
    }
}
