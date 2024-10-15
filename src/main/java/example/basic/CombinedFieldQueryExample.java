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
//
// ## Key Features of CombinedFieldQuery:
// - The search engine tries to "combine" those fields to make sense of the whole query.
// - When you have multiple fields that represent related data, like title, author, and description, and you want to treat them as one field during search.
// - Simplifies complex multi-field queries by treating fields as a unified text source.
// - Enhances relevance scoring by considering all specified fields in the context of the query.
// - When users perform searches with terms that might appear in different fields but are conceptually related.
//
// In the following example, we demonstrate how to use `CombinedFieldQuery` to perform a search over the fields `author`, `title`, and `description`.

package example.basic;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class CombinedFieldQueryExample {
    public static void main(String[] args) throws Exception {
        // ### Step 1: Create a new index
        // In-memory ByteBuffersDirectory is used for indexing
        Directory directory = new ByteBuffersDirectory();

        // Create the configuration for the index writer, using a standard analyzer
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

        // Create an IndexWriter to add documents to the index
        IndexWriter indexWriter = new IndexWriter(directory, config);

        // ### Step 2: Add documents to the index
        // Add sample documents with author (as a string), title, and description fields
        addDocument(indexWriter, "J.K. Rowling",    "Harry Potter and the Philosopher's Stone",  "A young wizard embarks on a magical journey.");
        addDocument(indexWriter, "J.R.R. Tolkien",   "The Hobbit: An Unexpected Journey",          "A hobbit sets off on an epic quest with a band of dwarves.");
        addDocument(indexWriter, "George Orwell",     "Nineteen Eighty-Four",                       "A novel depicting a society under constant surveillance and control.");
        addDocument(indexWriter, "F. Scott Fitzgerald", "The Magnificent Gatsby",                    "A story of wealth, passion, and the American Dream.");
        addDocument(indexWriter, "Harper Lee",        "To Kill a Mockingbird: A Story of Injustice", "A narrative about racial inequality and moral awakening in the South.");
        addDocument(indexWriter, "Jane Austen",       "Pride and Prejudice: Love and Class",       "A romantic tale that delves into themes of social class and relationships.");
        addDocument(indexWriter, "Mark Twain",        "The Adventures of Huckleberry Finn",         "The escapades of a boy journeying down the Mississippi River.");
        addDocument(indexWriter, "Agatha Christie",   "Murder on the Express Train",                "A mystery novel featuring the famous detective Hercule Poirot.");
        addDocument(indexWriter, "Gabriel García Márquez", "One Hundred Years of Solitude: A Family Saga", "A generational saga of the Buendía family in the mythical town of Macondo.");
        addDocument(indexWriter, "F. Scott Fitzgerald", "This Side of Paradise: A Novel of Youth",  "A narrative exploring the life and romances of Amory Blaine.");
        addDocument(indexWriter, "Khaled Hosseini",   "The Kite Runner: A Story of Redemption",     "A tale of friendship and forgiveness set against the backdrop of Afghanistan.");
        addDocument(indexWriter, "George R.R. Martin", "A Clash of Kings",                           "The second book in a fantasy series about the battle for the Iron Throne.");
        addDocument(indexWriter, "Herman Melville",   "Moby Dick: The Whale",                       "The journey of Captain Ahab as he hunts the elusive white whale.");
        addDocument(indexWriter, "C.S. Lewis",        "The Chronicles of Narnia: The Lion, the Witch, and the Wardrobe", "A fantasy series about a magical land filled with adventure.");
        addDocument(indexWriter, "J.D. Salinger",     "The Catcher in the Rye: A Teenage Tale",    "A story highlighting adolescent alienation and rebellion.");
        addDocument(indexWriter, "Chinua Achebe",     "Things Fall Apart: A Tale of Tradition",     "A narrative examining the effects of colonialism on African culture.");
        addDocument(indexWriter, "Ray Bradbury",      "Fahrenheit 451: A Future Without Books",     "A dystopian tale about a world where reading is forbidden.");
        addDocument(indexWriter, "Virginia Woolf",    "Mrs. Dalloway: A Day in London",            "A narrative that portrays a woman's life and thoughts in post-war England.");


        // Close the IndexWriter after adding all documents
        indexWriter.close();

        // ### Step 3: Search the index
        // Open a DirectoryReader to read the index
        DirectoryReader reader = DirectoryReader.open(directory);

        // Create an IndexSearcher to perform searches on the indexed data
        IndexSearcher searcher = new IndexSearcher(reader);

        // ### Step 4: Execute different types of queries

        // 1. CombinedFieldQuery: Treats multiple fields as one and combines term frequencies
        CombinedFieldQuery.Builder combinedFieldQueryBuilder = new CombinedFieldQuery.Builder();
        combinedFieldQueryBuilder.addField("author").addField("title").addField("description");
        combinedFieldQueryBuilder.addTerm(new BytesRef("Rowling"));
        combinedFieldQueryBuilder.addTerm(new BytesRef("Potter"));
        combinedFieldQueryBuilder.addTerm(new BytesRef("magical"));
        Query combinedFieldQuery = combinedFieldQueryBuilder.build();

        // 2. BooleanQuery: Separate queries for each field with OR (SHOULD) clauses
        BooleanQuery.Builder boolQueryBuilder = new BooleanQuery.Builder();
        for (String field : List.of("author", "title", "description")) {
            for (String term : List.of("Rowling", "Potter", "magical")) {
                boolQueryBuilder.add(new TermQuery(new Term(field, term)), BooleanClause.Occur.SHOULD);
            }
        }
        BooleanQuery boolQuery = boolQueryBuilder.build();

        // 3. docCombinedFieldQuery: Querying the "combined_field" field where all terms are combined which should have the same result as CombinedFieldQuery
        BooleanQuery docCombinedFieldQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("combined_field", "Rowling")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("combined_field", "Potter")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("combined_field", "magical")), BooleanClause.Occur.SHOULD)
                .build();

        // ### Step 5: Execute and print results for each query type
        //#### CombinedFieldQuery Results:
        //Score: 0.98660445, Author: J.K. Rowling, Title: Harry Potter and the Philosopher's Stone, Description: A young wizard embarks on a magical journey.
        //Score: 0.8499142, Author: C.S. Lewis, Title: The Chronicles of Narnia: The Lion, the Witch, and the Wardrobe, Description: A fantasy series about a magical land filled with adventure.
        System.out.println("### CombinedFieldQuery Results:");
        printResults(searcher, combinedFieldQuery);

        //#### BooleanQuery Results:
        //Score: 1.0287321, Author: J.K. Rowling, Title: Harry Potter and the Philosopher's Stone, Description: A young wizard embarks on a magical journey.
        //Score: 0.9480082, Author: C.S. Lewis, Title: The Chronicles of Narnia: The Lion, the Witch, and the Wardrobe, Description: A fantasy series about a magical land filled with adventure.
        System.out.println("\n### BooleanQuery Results:");
        printResults(searcher, boolQuery);

        //#### DocCombinedFieldQuery Results:
        // Score: 0.98660445, Author: J.K. Rowling, Title: Harry Potter and the Philosopher's Stone, Description: A young wizard embarks on a magical journey.
        // Score: 0.8499142, Author: C.S. Lewis, Title: The Chronicles of Narnia: The Lion, the Witch, and the Wardrobe, Description: A fantasy series about a magical land filled with adventure.
        System.out.println("\n### DocCombinedFieldQuery Results:");
        printResults(searcher, docCombinedFieldQuery);

        reader.close();
    }

    // ### Helper method to print search results
    private static void printResults(IndexSearcher searcher, Query query) throws IOException {
        TopDocs topDocs = searcher.search(query, 10);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = searcher.storedFields().document(scoreDoc.doc);
            System.out.println("Score: " + scoreDoc.score +
                    ", Author: " + doc.get("author") +
                    ", Title: " + doc.get("title") +
                    ", Description: " + doc.get("description"));
        }
    }

    // ### Helper method to add a document to the index
    private static void addDocument(IndexWriter indexWriter, String author, String title, String description) throws IOException {
        // Create a new document
        Document doc = new Document();
        // Add the author field as a string (stored)
        doc.add(new TextField("author", author, Field.Store.YES));
        // Add the title field as a text field (stored)
        doc.add(new TextField("title", title, Field.Store.YES));
        // Add the description field as a text field (stored)
        doc.add(new TextField("description", description, Field.Store.YES));
        // Add combined_field in one field
        doc.add(new TextField("combined_field", String.format(Locale.ROOT, "%s %s %s", author, title, description), Field.Store.YES));
        // Add the document to the index
        indexWriter.addDocument(doc);
    }
}
