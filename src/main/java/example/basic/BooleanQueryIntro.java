// # Boolean query introduction
//
// While term queries are the basic build block for searching indexed text, Boolean queries are the most common way
// to combine multiple smaller queries to implement more complex matching logic. Though the implementing class is
// called `BooleanQuery`, they don't implement true Boolean logical operators (binary AND, binary OR, unary NOT).
// Instead, Lucene's Boolean queries use four unary operators:
//
// 1. `MUST`: Indicates that a clause must match for the Boolean query to be satisfied. The similarity score of the
//    match in the document is counted toward the document's score. Usually applied to clauses derived from user input,
//    `MUST` clauses say "This is what I'm looking for in a document".
// 2. `SHOULD`: Indicates that a match on the clause is optional, but if a document matches then the match counts
//    toward the document's score. If a query contains `SHOULD` clauses, but no `MUST` or `FILTER` clauses, then at
//    least one `SHOULD` clause must match (i.e. they are effectively ORed together). You can set the
//    `minimumNumberShouldMatch` property to indicate that some number of `SHOULD` clauses must be satisfied to get a
//    match. In the general case, `SHOULD` indicates that a clause is nice to have, but not a requirement.
// 3. `FILTER`: Similar to `MUST`, any `FILTER` clause must match, but a matching `FILTER` clause will not be counted
//    toward the document's score. These clauses are added to restrict the result set without saying anything about
//    the value of the matching clause. Examples include restricting documents by a date range, or limiting to
//    documents with the color red, etc.
// 4. `MUST_NOT`: Excludes documents that match the given clause. Like `FILTER`, these clauses do not impact document
//    scores, but change the returned results.
//
package example.basic;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BooleanQueryIntro {
    // ## Document construction
    //
    // We will create some text documents containing the prime divisors (less than ten) of their doc IDs (and `one`).
    // That is, the document with ID 30 will contain "one two three five".
    private static List<String> createDocumentText(int numDocs) {
        List<String> docs = new ArrayList<>();
        docs.add("zero"); /* First doc will just have "zero", since the first doc ID is 0 */
        for (int i = 1; i < numDocs; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("one");
            if (i % 2 == 0) {
                sb.append(" two");
            }
            if (i % 3 == 0) {
                sb.append(" three");
            }
            if (i % 5 == 0) {
                sb.append(" five");
            }
            if (i % 7 == 0) {
                sb.append(" seven");
            }
            docs.add(sb.toString());
        }
        return docs;
    }

    // ## Simplifying scores
    //
    // When I first tried writing this example, the scores were values like 0.9077946 and 0.7806307, because they were
    // using BM25 similarity, which has the following properties:
    //
    // 1. A less common term (like "seven") is worth more than a more common term (like "one", which occurs in every
    //    document except doc 0).
    // 2. A match on a longer document (like a multiple of 210) is worth less than a match on a shorter document.
    // 3. A term occurring more times in a document is worth more (which is not an issue in this example, since each
    //    term is added to a document once).
    //
    // To make things simpler, we can override the similarity function used for scoring. In this case, any matching
    // scoring clause is given a score of 1. The score of a document will correspond to the number of matching scoring
    // clauses. Note that "scoring clauses" are MUST or SHOULD.
    //
    private static class CountMatchingClauseSimilarity extends SimilarityBase {
        @Override
        protected double score(BasicStats stats, double freq, double docLen) {
            return 1;
        }

        @Override
        public String toString() {
            return "Everything is 1";
        }
    }

    // ## Output helper
    //
    // We're going to run a bunch of queries and output documents and scores. While we could use `searcher.search(...)`,
    // that will (by default) sort the documents by descending score. In this example, I believe it's clearer if we
    // see the matching documents ordered by their doc IDs, which have the divisors that we listed above.
    //
    // This also provides an opportunity to explain some of what happens under the hood when you call `search`.
    //
    // The `Query` object has no knowledge of the index or its contents. When we call `createWeight` on the
    // `IndexSearcher`, it passes itself into the `createWeight` implementation of the `Query`. The resulting `Weight`
    // object is like a "prepared" version of the query, possibly based on information derived from the searcher, but
    // also based on the `ScoreMode` passed in.
    //
    // For each segment (leaf) of the index, we ask the `Weight` to provide a `Scorer`. The `Scorer` wraps
    // a `DocIdSetIterator`, which is able to iterator over the IDs of the documents in the segment that match the
    // query. The `Scorer` also has a `score()` method that returns the score of the current document.
    //
    private static void outputSearchResultsAndScores(IndexSearcher searcher, Query query) throws IOException {
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
        for (LeafReaderContext lrc : searcher.getIndexReader().leaves()) {
            Scorer scorer = weight.scorer(lrc);
            DocIdSetIterator docIdSetIterator = scorer.iterator();
            int docId;
            while ((docId = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                System.out.println(docId + " " + scorer.score());
            }
        }
    }

    // ## Boolean query examples
    //
    // Let's use the helpers above to see how Boolean queries match and score documents. We create 1000 documents
    // (with doc IDs 0 through 999) and add them to an index. We also need to pass our custom `Similarity`
    // implementation to the `IndexSearcher` to override the default BM25 similarity.
    public static void main(String[] args) throws IOException {
        Path tmpDir = Files.createTempDirectory(BooleanQueryIntro.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir);
             IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (String doc : createDocumentText(1000)) {
                writer.addDocument(List.of(new TextField("text", doc, Field.Store.NO)));
            }

            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setSimilarity(new CountMatchingClauseSimilarity());

                // ### Pure conjunction (AND queries)
                //
                // Let's construct a query that matches all documents that are multiples of 2 and 5 (i.e. multiples
                // of 10). All matching documents will have score 2.0, because exactly two clauses match.
                //
                BooleanQuery twoAndFive = new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "five")), BooleanClause.Occur.MUST)
                        .build();
                System.out.println(twoAndFive.toString());
                outputSearchResultsAndScores(searcher, twoAndFive);

                // ### Excluding a clause (NOT)
                //
                // Let's repeat that experiment, but this time, we will exclude documents that are multiples of 3.
                // The returned documents will all still have score 2.0.
                //
                BooleanQuery twoAndFiveNotThree = new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "five")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "three")), BooleanClause.Occur.MUST_NOT)
                        .build();
                System.out.println(twoAndFiveNotThree.toString());
                outputSearchResultsAndScores(searcher, twoAndFiveNotThree);

                // ### Boosting a clause (SHOULD)
                //
                // We'll do that again, but we will give a boost to documents that are multiples of 7. So, most
                // of the matches will have score 2.0, but 70, 140, 280, etc. will have score 3.0. (We skip 210 because
                // it's a multiple of 3.)
                //
                BooleanQuery twoAndFiveNotThreeMaybeSeven = new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "five")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "three")), BooleanClause.Occur.MUST_NOT)
                        .add(new TermQuery(new Term("text", "seven")), BooleanClause.Occur.SHOULD)
                        .build();
                System.out.println(twoAndFiveNotThreeMaybeSeven.toString());
                outputSearchResultsAndScores(searcher, twoAndFiveNotThreeMaybeSeven);

                // ### Filtering without scoring (FILTER)
                //
                // Let's repeat that last example, but we'll switch "five" from `MUST` to `FILTER`. Most clauses will
                // now have score 1.0, but the multiples of 7 have score 2.0.
                //
                BooleanQuery twoFilterFiveNotThreeMaybeSeven = new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "five")), BooleanClause.Occur.FILTER)
                        .add(new TermQuery(new Term("text", "three")), BooleanClause.Occur.MUST_NOT)
                        .add(new TermQuery(new Term("text", "seven")), BooleanClause.Occur.SHOULD)
                        .build();
                System.out.println(twoFilterFiveNotThreeMaybeSeven.toString());
                outputSearchResultsAndScores(searcher, twoFilterFiveNotThreeMaybeSeven);

                // ### Pure disjunction (OR queries)
                //
                // Remember from the introduction to this example that BooleanQueries do not implement pure Boolean
                // logic. The `SHOULD` clauses above were not saying "OR seven". The only way to get "OR" behavior is
                // through a BooleanQuery with no MUST or FILTER clauses. The following will output all multiples of
                // 2 or 7. Most docs will have score 1.0, but multiples of 14 will have score 2.0.
                //
                BooleanQuery twoOrSeven = new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term("text", "seven")), BooleanClause.Occur.SHOULD)
                        .build();
                System.out.println(twoOrSeven.toString());
                outputSearchResultsAndScores(searcher, twoOrSeven);

                // ### Combining conjunctions with disjunctions
                //
                // What if we do want to say "(two AND five) OR seven"? The OR still must be part of a pure disjunction,
                // but we can nest the conjunction under the disjunction. The following will output all multiples of
                // 10 or 7. The multiples of 7 generally have score 1.0, the multiples of 10 generally have score 2.0,
                // but the multiples of 70 have score 3.0.
                //
                BooleanQuery twoAndFiveOrSeven = new BooleanQuery.Builder()
                        .add(new BooleanQuery.Builder()
                                .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.MUST)
                                .add(new TermQuery(new Term("text", "five")), BooleanClause.Occur.MUST)
                                .build(), BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term("text", "seven")), BooleanClause.Occur.SHOULD)
                        .build();
                System.out.println(twoAndFiveOrSeven.toString());
                outputSearchResultsAndScores(searcher, twoAndFiveOrSeven);
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            Files.deleteIfExists(tmpDir);
        }

    }
}
