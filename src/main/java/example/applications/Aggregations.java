// # Aggregations
//
// The source code for this example is available at https://github.com/msfroh/lucene-university/blob/main/src/main/java/example/applications/Aggregations.java
//
// While Lucene was originally designed for full-text search, it has grown to support a wide range of applications,
// including analytics. Deployable services like OpenSearch and Elasticsearch use Lucene as the underlying engine, but
// offer powerful features to analyze logs and other data. A common use case is to ingest logs and then visualize trends
// over time, such as the number of errors per minute.
//
// This is done using aggregations, which are a way to summarize and analyze data in bulk. The basic premise of
// aggregations is that you filter documents with a query, and then collect the results into buckets based on some
// criteria. Lucene handles the low-level filtering and matching of documents, while providing the `Collector` interface
// that high-level services can use to customize how results are collected. A standard search use-case collects the
// top N documents by sweeping through all documents and collecting them into a priority queue (with some neat tricks
// to skip blocks of documents that are not competitive). Aggregations tend to step through all matching documents,
// and count them according to the bucketing criteria.
//
// In this example, we'll implement a simple date histogram aggregation that counts the number of documents matching
// our query and count them based on which time interval they fall into. This is a common use case for log analysis,
// where you would probably render that date histogram as a graph of counts (on the y-axis) over time (on the x-axis).

package example.applications;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Aggregations {

    // ## Data
    //
    // The first thing we'll need to have an interesting example is some data. Like in many previous examples, we'll
    // generate some documents. In earlier examples, I've written a method that returns a list of documents, but in
    // this case, I want a lot of documents, so I'm going to generate documents and pass them directly to the
    // `IndexWriter`, so they don't have to live in memory.
    //
    // For a slightly realistic example, we'll generate documents with roughly increasing timestamps, but also some
    // "jitter" to reflect the fact that logs don't usually arrive in a perfectly ordered manner. We'll split the
    // documents between `ERROR` status and `INFO` status. To make things interesting, I'll try to increase the number
    // of `ERROR` documents near the middle of the document range (which will roughly correspond to the middle
    // timestamps).
    //
    // To split documents across multiple Lucene segments, we'll flush the `IndexWriter` every 10,000
    // documents.

    private static void generateDocuments(IndexWriter indexWriter, int docCount) throws IOException {
        long baseTimestamp = System.currentTimeMillis();
        List<IndexableField> doc = new ArrayList<>();
        Random random = new Random();
        int middle = docCount / 2;
        for (int i = 0; i < docCount; i++) {
            long timestamp = baseTimestamp + random.nextInt(100000) + (i / 100) * 1000L;
            doc.add(new LongField("timestamp", timestamp, Field.Store.NO));
            float errorChance =  0.05f * 2.0f - (float) Math.abs(middle - i) / docCount;
            doc.add(new KeywordField("status", random.nextFloat() < errorChance? "ERROR" : "INFO", Field.Store.NO));
            indexWriter.addDocument(doc);
            doc.clear();
            if (i % 10_000 == 0 && i > 0) {
                indexWriter.flush();
            }
        }
    }

    // ## Searching
    //
    // The next building block is the code to search for the documents. We'll implement the aggregation as a custom
    // collector, which we'll pass in via a custom `CollectorManager`. The logic in this method will mostly reimplement
    // what's in `IndexSearcher#search(Query, CollectorManager)`, but I like to "unwrap" it to make the whole process
    // more obvious.
    //
    // Lucene also has an `IndexSearcher#search(Query, Collector)` method, but that's been deprecated since
    // multi-threaded search became the norm. The contract of the `Collector` interface doesn't require that it be
    // thread-safe, so `CollectorManager` was introduced to create a `Collector` for each thread. Since we're using
    // `CollectorManager`, I'll go ahead and implement a multi-threaded search, where we distribute segments across
    // threads. By default, Lucene uses a slightly more complicated strategy to divide work across threads, which you
    // can see in the `IndexSearcher.slices(...)` static method.
    //
    // First, we'll implement the code that does the work for a single thread. Given a collection of segments (or
    // "leaves"), for each one the thread should create a `LeafCollector` and then use the `Weight` for the query to
    // get a `ScorerSupplier`, which it can use to get a `Scorer`. The `Scorer` has a `DocIdSetIterator`, which we can
    // use to iterate through the matching documents in the segment. For each matching document, we pass its ID to the
    // `LeafCollector#collect` method. Again, don't implement this from scratch -- for each segment this is similar to
    // what `IndexSearcher#searchLeaf` does (though it uses `BulkScorer` in case the query has specialized bulk logic).
    private static void searchSegments(List<LeafReaderContext> segments, Weight weight, Collector collector) {
        for (LeafReaderContext context : segments) {
            try {
                LeafCollector leafCollector = collector.getLeafCollector(context);
                if (leafCollector == null) {
                    continue; /* Skip segments that won't be collected. */
                }
                ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
                if (scorerSupplier == null) {
                    continue; /* Skip segments that don't match the query. */
                }
                Scorer scorer = scorerSupplier.get(Long.MAX_VALUE);
                DocIdSetIterator disi = scorer.iterator();
                int docId;
                while ((docId = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    leafCollector.collect(docId);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Now we dispatch work to individual threads in the `search` method, which manages a thread pool to run the
    // query. In real code, you would keep a long-lived pool of search threads, rather than creating one on each
    // search. As mentioned, we'll also just approximately balance segments across threads, rather than doing something
    // smarter, like balancing based on the number of documents in each segment. For each thread, we create a new
    // `Collector` and call the `searchSegments` method to do the work. After dispatching all the threads, we wait for
    // them to finish and ask `CollectorManager` to reduce the results from all threads into a single result. Note that
    // Lucene's `IndexSearcher#search` method doesn't just block the calling thread waiting for the worker threads to
    // finish, but rather gives it the last slice of work. Only once the calling thread has finished its work does it
    // wait for the worker threads to finish.
    private static <C extends Collector, T> T search(IndexReader indexReader,
                                                     Query query,
                                                     CollectorManager<C, T> collectorManager,
                                                     int numThreads) throws IOException {

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        Query rewrittenQuery = indexSearcher.rewrite(query);
        Weight weight = rewrittenQuery.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        int segmentsPerThread = (int) Math.ceil((double) indexReader.leaves().size() / numThreads);

        /* Don't really create a new thread pool on every search. This is just for the example. */
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            List<C> collectors = new ArrayList<>();
            List<LeafReaderContext> threadSegments = new ArrayList<>();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < indexReader.leaves().size(); i++) {
                threadSegments.add(indexReader.leaves().get(i));
                if (threadSegments.size() == segmentsPerThread) {
                    C collector = collectorManager.newCollector();
                    collectors.add(collector);
                    final List<LeafReaderContext> threadSegmentsFinal = new ArrayList<>(threadSegments);
                    futures.add(executor.submit(() -> searchSegments(threadSegmentsFinal, weight, collector)));
                    threadSegments = new ArrayList<>();
                }
            }
            /* Pick up any remaining segments if we ran out before filling the last thread. */
            if (threadSegments.size() > 0) {
                C collector = collectorManager.newCollector();
                collectors.add(collector);
                final List<LeafReaderContext> threadSegmentsFinal = new ArrayList<>(threadSegments);
                futures.add(executor.submit(() -> searchSegments(threadSegmentsFinal, weight, collector)));
            }
            /* Wait for all threads to finish */
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException("Error in thread execution", e);
                }
            }
            return collectorManager.reduce(collectors);
        }
    }

    // ## Time-Bucketing Aggregation Collector
    //
    // While the code above provides a generic framework for running a query and collecting results (since it's kind
    // of a simplified version of `IndexSearcher#search`), we still need to implement the actual aggregation logic as
    // a custom `Collector`. For now, I'm going to implement a simple time-bucketing collector that counts the number of
    // documents in each time interval. This will be similar to the `data_histogram` aggregation in OpenSearch and
    // Elasticsearch, but without nearly as many features.
    //
    // Let's start with the boring parts of our `Collector` implementation. We'll need to read from some field and
    // collect document counts into buckets. In this case, we'll take a start time, a bucket/interval size, and the
    // count of buckets.
    private static class TimeBucketCollector implements Collector {
        private final String fieldName;
        private final long startTimeMillis;
        private final long bucketSizeMillis;
        private final long[] buckets;

        public TimeBucketCollector(String fieldName, long startTimeMillis, long bucketSizeMillis, int numBuckets) {
            this.fieldName = fieldName;
            this.startTimeMillis = startTimeMillis;
            this.bucketSizeMillis = bucketSizeMillis;
            this.buckets = new long[numBuckets];
        }

        // Collectors must implement the `scoreMode` method, which indicates whether scores are needed. In our case,
        // we're just counting documents, so we don't need scores.

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        // Now we get to the interesting part, where we implement the `LeafCollector` interface. The anonymous
        // `LeafCollector` that we create here is created for each segment (or "leaf") of the index. When we start
        // processing a segment, we load the `SortedNumericDocValues` for the field we're interested in. As we collect
        // each document that matches the query, we check if it has a value for the field (based on the return value
        // of `advanceExact(docId)`). Since a document could have multiple values for the field, we loop through all of
        // them and calculate the bucket index based on the timestamp value. We then increment the count for that bucket.
        //
        // It's worth highlighting that the `LeafCollector` is created per-segment, but the `Collector` (and its
        // `buckets`) spans multiple segments. From a thread-safety perspective, we're still okay, since the `Collector`
        // is only accessed by a single thread.
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedNumericDocValues fieldValues = context.reader().getSortedNumericDocValues(fieldName);
            if (fieldValues == null) {
                return null;
            }
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    /* We don't need doc scores, so we can ignore the scorer. */
                }

                @Override
                public void collect(int docId) throws IOException {
                    if (fieldValues.advanceExact(docId)) {
                        for (int i = 0; i < fieldValues.docValueCount(); i++) {
                            long timestamp = fieldValues.nextValue();
                            int bucketIndex = Math.toIntExact((timestamp - startTimeMillis) / bucketSizeMillis);
                            if (bucketIndex >= 0 && bucketIndex < buckets.length) {
                                // Increment the count for the bucket corresponding to this timestamp.
                                buckets[bucketIndex]++;
                            }
                        }
                    }
                }
            };
        }


        // Finally, we'll need a way to return the buckets after the collection is done. This will be used by the
        // `reduce` phase in `CollectorManager`, which combines results from all threads. This is implemented below.

        long[] getBuckets() {
            return buckets;
        }
    }

    // Letting our `TimeBucketCollector` work across multiple threads requires a `CollectorManager` that creates
    // a new `TimeBucketCollector` for each thread. The `reduce` method will then combine the results from all threads
    // into a single result.
    private static class TimeBucketCollectorManager implements CollectorManager<TimeBucketCollector, long[]> {
        private final String fieldName;
        private final long startTimeMillis;
        private final long bucketSizeMillis;
        private final int numBuckets;

        public TimeBucketCollectorManager(String fieldName, long startTimeMillis, long endTimeMillis, long bucketSizeMillis) {
            this.fieldName = fieldName;
            this.startTimeMillis = startTimeMillis;
            this.bucketSizeMillis = bucketSizeMillis;
            this.numBuckets = Math.toIntExact((endTimeMillis - startTimeMillis) / bucketSizeMillis) + 1;
        }

        @Override
        public TimeBucketCollector newCollector() {
            return new TimeBucketCollector(fieldName, startTimeMillis, bucketSizeMillis, numBuckets);
        }

        @Override
        public long[] reduce(Collection<TimeBucketCollector> collectors) {
            long[] combinedBuckets = new long[numBuckets];
            for (TimeBucketCollector collector : collectors) {
                long[] buckets = collector.getBuckets();
                for (int i = 0; i < buckets.length; i++) {
                    combinedBuckets[i] += buckets[i];
                }
            }
            return combinedBuckets;
        }
    }

    // ## Putting It All Together
    //
    // The above classes implement the logic for a time-bucketing aggregation. Now we can put it all together and see
    // if it works.
    //
    public static void main(String[] args) throws IOException {
        Path indexDir = Files.createTempDirectory(Aggregations.class.getSimpleName());
        try (Directory dir = FSDirectory.open(indexDir);
             IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig());
        ) {
            generateDocuments(indexWriter, 10_000_000);

            try (IndexReader indexReader = DirectoryReader.open(indexWriter)) {
                Query query = new TermQuery(new Term("status", "ERROR"));
                long startTimeMillis = System.currentTimeMillis();
                long endTimeMillis = System.currentTimeMillis() + (24 * 60 * 60 * 1000);
                long bucketSizeMillis = 60 * 60 * 1000;

                TimeBucketCollectorManager collectorManager = new TimeBucketCollectorManager(
                    "timestamp",
                    startTimeMillis,
                    endTimeMillis,
                    bucketSizeMillis
                );

                int numThreads = 4;
                long[] buckets = search(indexReader, query, collectorManager, numThreads);

                // Print the results.
                for (int i = 0; i < buckets.length; i++) {
                    System.out.printf("Bucket %d: %d documents%n", startTimeMillis + i * bucketSizeMillis, buckets[i]);
                }
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(indexDir)) {
                Files.deleteIfExists(indexDir.resolve(indexFile));
            }
            Files.delete(indexDir);
        }
    }

    // ## Output
    //
    // Running the above code gives me the following output (the exact numbers will vary due to the random nature of
    // the document generation):
    // ```
    // Bucket 1753399180866: 0 documents
    // Bucket 1753402780866: 0 documents
    // Bucket 1753406380866: 0 documents
    // Bucket 1753409980866: 0 documents
    // Bucket 1753413580866: 0 documents
    // Bucket 1753417180866: 0 documents
    // Bucket 1753420780866: 0 documents
    // Bucket 1753424380866: 0 documents
    // Bucket 1753427980866: 0 documents
    // Bucket 1753431580866: 0 documents
    // Bucket 1753435180866: 0 documents
    // Bucket 1753438780866: 4936 documents
    // Bucket 1753442380866: 17821 documents
    // Bucket 1753445980866: 30564 documents
    // Bucket 1753449580866: 28441 documents
    // Bucket 1753453180866: 15050 documents
    // Bucket 1753456780866: 3014 documents
    // Bucket 1753460380866: 0 documents
    // Bucket 1753463980866: 0 documents
    // Bucket 1753467580866: 0 documents
    // Bucket 1753471180866: 0 documents
    // Bucket 1753474780866: 0 documents
    // Bucket 1753478380866: 0 documents
    // Bucket 1753481980866: 0 documents
    // Bucket 1753485580866: 0 documents
    // ```


    // ## Going deeper
    //
    // The above example shows how to implement a simple time-based aggregation. In a future example, we'll go into
    // sub-aggregations -- that is, we bucket documents according to one criterion, then again by another.
}
