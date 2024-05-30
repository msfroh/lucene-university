// # Conjunctive Boolean query (AND) internals
//
// This example partly builds on top of the concepts introduced in `BooleanQueryIntro`, so I suggest you go check
// that example first.
//
// If you happen to have arrived here through a web search, please **DO NOT ACTUALLY USE THIS CODE**. Lucene already
// has a BooleanQuery class. See the neighboring `BooleanQueryIntro` example for some guidance on using it.
//
// This example digs into some of the implementation details for Boolean queries, specifically conjunctive (`AND`)
// queries. Since Lucene's real Boolean queries are quite sophisticated, most of this example will involve building
// our own version of a Boolean query that behaves like a binary "AND" operator. That is, it has exactly two clauses,
// which are both required (and participate in scoring). Along the way, we'll learn some things about how Lucene
// queries in general are implemented, and how Boolean queries implement "leap-frogging".
//
// Before we get into the code, here is an overview of the chain of classes we need to implement for our own query:
// 1. `Query`: A query represents the instruction for *what* we want to match/score, without getting into the details
//             of *how* we're going to do so.
// 2. `Weight`: Returned by the `createWeight` method on the query. The JavaDoc for `Weight` says, "The purpose of
//             Weight is to ensure searching does not modify a Query, so that a Query instance can be reused." While
//             the `Query` is independent of any index state, the `Weight` is constructed based on the current
//             `IndexSearcher`, and properties of the search action (e.g. whether scoring is needed).
// 3. `Scorer`: For each index segment, the `Weight` creates a `Scorer`. The `Scorer` may be used once to step through
//             matching documents in the segment (via its embedded `DocIdSetIterator` -- explained next), and returns
//             the score (at this query's level) for the document.
// 4. `DocIdSetIterator`: A `DocIdSetIterator` (often abbreviated to DISI) steps through matching document IDs for the
//             given segment in increasing order. Many DISIs support an efficient `advance(int n)` method that returns
//             the matching doc ID greater than or equal to `n`.
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
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
import java.util.Objects;

public class BooleanQueryANDInternals {
    // ## Document construction
    //
    // We'll reuse the documents from `BooleanQueryIntro`. In brief, the documents contain the words for their
    // prime factors less than 10.
    //
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

    // ## Query class
    //
    // Let's start with our custom query class. Since we want to represent the specific `a AND b` case, we will
    // explicitly take `a` and `b` as "left" and "right" operands.
    //
    private static final class BinaryAndQuery extends Query {
        private final Query left;
        private final Query right;

        public BinaryAndQuery(Query left, Query right) {
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
        }

        // A query should implement a friendly `toString` method that provides a human-readable representation.
        @Override
        public String toString(String field) {
            return left.toString(field) + " AND " + right.toString(field);
        }

        // To allow operations on complex query hierarchies, queries are expected to take a `QueryVisitor` and pass
        // it down the tree. The "leaf" queries at the end of a query hierarchy should call `visitor.visitLeaf(this)`.
        // Functionally, this implementation is identical to what `BooleanQuery` will do with a pair of `MUST` clauses.
        @Override
        public void visit(QueryVisitor visitor) {
            QueryVisitor childVisitor = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
            left.visit(childVisitor);
            right.visit(childVisitor);
        }

        // Since a `Query` may be used as a hash key (e.g. if the results of the query are cached), they are expected
        // to correctly implement `equals` and `hashCode`.
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BinaryAndQuery that = (BinaryAndQuery) o;
            return Objects.equals(left, that.left) && Objects.equals(right, that.right);
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }

        // Finally, when it comes time to search with our query, we create the `Weight` instance. Note that
        // we need to call `createWeight` on each of our nested queries, too. Note that while we could call
        // `createWeight` on the `left` and `right` queries directly, it is more correct to delegate to the `searcher`,
        // which may have one of the nested queries in its cache. (See the `isCacheable` method in `BinaryAndWeight`,
        // below.)
        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new BinaryAndWeight(this, searcher.createWeight(left, scoreMode, boost), searcher.createWeight(right, scoreMode, boost));
        }
    }

    // ## Weight class
    //
    // For our purposes, the role of the `Weight` implementation is to hold the `Weight` instances for the operands,
    // create the `Scorer` for each segment, and implement the `explain` method.
    //
    private static class BinaryAndWeight extends Weight {
        private final Weight leftWeight;
        private final Weight rightWeight;

        public BinaryAndWeight(Query query, Weight leftWeight, Weight rightWeight) {
            super(query);
            this.leftWeight = leftWeight;
            this.rightWeight = rightWeight;
        }

        // Every `Weight` is expected to implement the `explain` method to show how a given document's score was
        // derived (or to explain why a non-matching document did not match).
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation leftExplain = leftWeight.explain(context, doc);
            Explanation rightExplain = rightWeight.explain(context, doc);
            if (!leftExplain.isMatch()) {
                return Explanation.noMatch("no match on required clause (" + leftWeight.getQuery().toString() + ")", leftExplain);
            } else if (!rightExplain.isMatch()) {
                return Explanation.noMatch("no match on required clause (" + rightWeight.getQuery().toString() + ")", rightExplain);
            }
            Scorer scorer = scorer(context);
            int advanced = scorer.iterator().advance(doc);
            assert advanced == doc;
            return Explanation.match(scorer.score(), "sum of:", leftExplain, rightExplain);
        }

        // We return our custom `Scorer` implementation for a given index segment, after creating the scorers for
        // each of our operands.
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            ScorerSupplier leftScorerSupplier = leftWeight.scorerSupplier(context);
            ScorerSupplier rightScorerSupplier = leftWeight.scorerSupplier(context);
            Weight weight = this;
            return new ScorerSupplier() {
                @Override
                public Scorer get(long l) throws IOException {
                    return new BinaryAndScorer(weight, leftScorerSupplier.get(l), rightScorerSupplier.get(l));
                }

                @Override
                public long cost() {
                    // Worst case, we match everything from left and right.
                    return leftScorerSupplier.cost() + rightScorerSupplier.cost();
                }
            };
        }

        // The `IndexSearcher` holds a `QueryCache` instance that may store the matching doc IDs for a given query.
        // Not all queries are readily cacheable, though, so the `IndexSearcher` asks the weight if it is cacheable
        // or not.
        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return leftWeight.isCacheable(ctx) && rightWeight.isCacheable(ctx);
        }
    }

    // ### Scorer class
    //
    // Next, we need to implement the `Scorer` class. Most of the interesting logic lives in the `Scorer`'s associated
    // `DocIdSetIterator`.
    private static class BinaryAndScorer extends Scorer {
        private final Scorer leftScorer;
        private final Scorer rightScorer;
        private final DocIdSetIterator docIdSetIterator;

        public BinaryAndScorer(Weight weight, Scorer leftScorer, Scorer rightScorer) {
            super(weight);
            this.leftScorer = leftScorer;
            this.rightScorer = rightScorer;
            this.docIdSetIterator = new BinaryAndDocIdSetIterator(leftScorer.iterator(), rightScorer.iterator());
        }

        // If this scorer is pointing at a doc ID, then both the left and right scorers must be pointing at the
        // same doc ID (since it's the current match for the left AND right).
        @Override
        public int docID() {
            assert leftScorer.docID() == rightScorer.docID();
            return docIdSetIterator.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return docIdSetIterator;
        }

        // This `getMaxScore` can be used to skip entire blocks of uncompetitive documents, by providing an upper bound
        // on the possible score of a match. In this case, we ask each of our subscorers what their maximum scores
        // are `upTo` some future target and sum them together. But the max from `left` and the max from `right` might
        // not come from the same document, so there may not actually be a document with this score. That's okay,
        // though -- the skipping is a best-effort optimization.
        @Override
        public float getMaxScore(int upTo) throws IOException {
            return leftScorer.getMaxScore(upTo) + rightScorer.getMaxScore(upTo);
        }

        // Consistent with BooleanQuery (using MUST clauses), we sum up the scores of our individual clauses.
        @Override
        public float score() throws IOException {
            assert leftScorer.docID() == rightScorer.docID();
            return leftScorer.score() + rightScorer.score();
        }
    }


    // ## The DocIdSetIterator class
    //
    // This class is really the whole reason for this example (though the journey was hopefully informative too).
    // The DocIdSetIterator (DISI) classes are fundamental to Lucene's matching logic.
    //
    // Conjunctions of required clauses (like our AND query) step through all documents where all of their underlying
    // DISIs point to the same thing. We achieve this with the (generally) fast `advance` method, asking a DISI
    // that's behind to skip to the first value greater than or equal to the DISI that's in the lead. If they point
    // to the same thing, then we have a match and return it. Otherwise, the old lead is now behind and it can leap
    // over the new lead, like a game of leapfrog.
    //
    // Note that our `advance` and `nextDoc` methods guarantee that they only return when `leftIterator` and
    // `rightIterator` point to the same value (though that value may be `NO_MORE_DOCS`).
    private static class BinaryAndDocIdSetIterator extends DocIdSetIterator {
        private final DocIdSetIterator leftIterator;
        private final DocIdSetIterator rightIterator;

        public BinaryAndDocIdSetIterator(DocIdSetIterator leftIterator, DocIdSetIterator rightIterator) {
            this.leftIterator = leftIterator;
            this.rightIterator = rightIterator;
        }

        // We can (and do) check the invariant that the left and right iterators are pointing to the same doc,
        // and return the left doc ID.
        @Override
        public int docID() {
            assert leftIterator.docID() == rightIterator.docID();
            return leftIterator.docID();
        }

        // For `nextDoc`, we arbitrarily step to the next document with `leftIterator`, then jump over to our
        // leapfrogging logic in `doNext`.
        @Override
        public int nextDoc() throws IOException {
            return doNext(leftIterator.nextDoc());
        }

        // As with `nextDoc`, we can `advance` the `leftIterator` and leapfrog in `doNext` until the iterators
        // converge.
        @Override
        public int advance(int target) throws IOException {
            return doNext(leftIterator.advance(target));
        }

        // Here it is, the exciting part of the example and one of the most important concepts in Lucene.
        // The left and right iterators jump over each other, with the lagging iterator advancing to the
        // first doc ID greater than or equal to the leading iterator. Several of the built-in query types (including
        // TermQuery) have efficient `advance` methods that can return in logarithmic time.
        //
        // This `while` loop is guaranteed to terminate, because eventually both iterators will point to `NO_MORE_DOCS`,
        // and passing `NO_MORE_DOCS` to an `advance` call is expected to return `NO_MORE_DOCS`. (The comparators work
        // because `NO_MORE_DOCS` is equal to `Integer.MAX_VALUE`.)
        private int doNext(int target) throws IOException {
            while (leftIterator.docID() != rightIterator.docID()) {
                if (rightIterator.docID() < leftIterator.docID()) {
                    rightIterator.advance(leftIterator.docID());
                } else {
                    leftIterator.advance(rightIterator.docID());
                }
            }
            return leftIterator.docID();
        }

        // The `cost()` of a DISI is a measure of how expensive it would be to step through it fully, generally
        // corresponding to an upper bound on the number of docs the DISI matches. We don't know how many documents
        // are in the intersection of `left` and `right` without stepping through them, but we know that the
        // intersection of `left` and `right` is definitely no bigger than whichever is smaller. (Picture the Venn
        // diagram of "A AND B".)
        //
        // One use of `cost()` is to lead with the cheapest (sparsest) iterator. Since our `advance` method always
        // steps with `left` first and our "AND" operator is commutative, we could have assigned `leftIterator`
        // to the lower-cost iterator to get a speed-up. (That's what Lucene's `ConjunctionDISI` class does.)
        @Override
        public long cost() {
            return Math.min(leftIterator.cost(), rightIterator.cost());
        }
    }

    // We can reuse the custom similarity from `BooleanQueryIntro` to give our example term queries scores of `1` on a
    // match, rather than worrying about BM25 scores.
    private static class CountMatchingClauseSimilarity extends SimilarityBase {
        @Override
        protected double score(BasicStats stats, double freq, double docLen) {
            return 1;
        }

        @Override
        public String toString() {
            return "Every match has a score of 1";
        }

        // While we didn't look at it in `BooleanQueryIntro`, a custom similarity can also explain its scoring
        // methodology. In our case, we can pass along the explanation from the `toString` method.
        @Override
        protected void explain(List<Explanation> subExpls, BasicStats stats, double freq, double docLen) {
            subExpls.add(Explanation.match(1, this.toString()));
        }
    }

    // ## Tying it all together
    //
    // Let's put our custom query type to work.
    //
    // After indexing documents 0-999, we create a `twoAndFive` query, similar to what we did in `BooleanQueryIntro`,
    // but this time it's using the logic implemented above.
    //
    // When the `IndexSearcher` (or technically the `DefaultBulkScorer`) first calls `nextDoc` on our
    // `DocIdSetIterator`, the left iterator ("two) will step to its first match -- document 2. Then the right ("five")
    // iterator steps to its first match greater than or equal to 2 -- document 5. Then the left iterator advances to
    // document 6. Then the right iterator advances to document 10. Finally, the left iterator advances to document 10
    // too, and we return it. (It's the first hit in the returned `TopDocs`.)
    //
    // The logic for the next (and every other) matching document is similar:
    // ```
    // left=12
    // right=15
    // left=16
    // right=20 <--
    // left=20 <-- HIT
    // ```
    //
    // After returning the top 10 hits, we can take a look at the `explain` output to see the output from our
    // custom `Weight` and custom `Similarity`.
    //
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

                BinaryAndQuery twoAndFive = new BinaryAndQuery(
                        new TermQuery(new Term("text", "two")),
                        new TermQuery(new Term("text", "five"))
                );
                System.out.println(twoAndFive);
                TopDocs topDocs = searcher.search(twoAndFive, 10);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    System.out.println(scoreDoc.doc + "  " + scoreDoc.score);
                }


                System.out.println(searcher.explain(twoAndFive, 20));
                /*
                2.0 = sum of:
                  1.0 = weight(text:two in 20) [CountMatchingClauseSimilarity], result of:
                    1.0 = score(CountMatchingClauseSimilarity, freq=1.0), computed from:
                      1 = Every match has a score of 1
                  1.0 = weight(text:five in 20) [CountMatchingClauseSimilarity], result of:
                    1.0 = score(CountMatchingClauseSimilarity, freq=1.0), computed from:
                      1 = Every match has a score of 1
                */

                System.out.println(searcher.explain(twoAndFive, 22));
                /*
                0.0 = no match on required clause (text:five)
                  0.0 = no matching term
                */
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            Files.deleteIfExists(tmpDir);
        }
    }


}
