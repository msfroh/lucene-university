package example.benchmarks;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class PointRangeThroughputMeasurement {
    private static final String TIMESTAMP_FIELD = "timestamp";

    private static BufferedReader openInputFile(Path inputPath) throws IOException {
        InputStream inputStream = Files.newInputStream(inputPath);
        if (inputPath.toString().endsWith(".gz")) {
            inputStream = new GZIPInputStream(inputStream);
        }
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    private static class PoolFiller implements Runnable {
        private final int[] finalQps;
        private final AtomicInteger pool;
        private final CountDownLatch stopLatch;
        private int currentQps;
        private int deltaQps;
        private int foundTarget = 0;


        public PoolFiller(int[] finalQps, AtomicInteger pool, CountDownLatch stopLatch, int startQPS, int startDelta) {
            this.finalQps = finalQps;
            this.pool = pool;
            this.stopLatch = stopLatch;
            this.currentQps = startQPS;
            this.deltaQps = startDelta;
        }

        @Override
        public void run() {
            int poolSize = pool.get();
            if (poolSize <= 0) {
                // Pool was drained, increase target QPS
                currentQps += deltaQps;
                System.out.println("Increasing QPS to " + currentQps);
                pool.addAndGet(currentQps);
            } else {
                // Pool was not drained, decrease target QPS and reduce delta
                deltaQps /= 2;
                if (deltaQps == 0) {
                    if (++foundTarget >= 5) {
                        finalQps[0] = currentQps;
                        stopLatch.countDown();
                    }
                    deltaQps = 1;
                }
                currentQps -= deltaQps;
                System.out.println("Decreasing QPS to " + currentQps);
                pool.set(currentQps);
            }
        }
    }

    private static class Worker implements Runnable {
        private final Query[] queries;
        private final IndexSearcher searcher;
        private final AtomicInteger pool;
        private final AtomicBoolean stop;
        private int pos;

        public Worker(Query[] queries, IndexSearcher searcher, int startPos, AtomicInteger pool, AtomicBoolean stop) {
            this.queries = queries;
            this.searcher = searcher;
            this.pos = startPos;
            this.pool = pool;
            this.stop = stop;
        }

        @Override
        public void run() {
            while (!stop.get()) {
                boolean wait = true;
                if (pool.get() > 0) {
                    int val = pool.decrementAndGet();
                    if (val >= 0) {
                        TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(10, Integer.MAX_VALUE);
                        try {
                            searcher.search(queries[pos++], manager);
                        } catch (IOException e) {
                            System.err.println(e.getMessage());
                        }
                        if (pos >= queries.length) {
                            pos = 0;
                        }
                        wait = false; // If we ran, then try to run again immediately
                    } else {
                        pool.incrementAndGet(); // Put back the token we took, since we're not using it
                    }
                }
                if (wait) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // Assume we won't get interrupted
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final int workerCount = Integer.parseInt(args[1]);
        Path tmpDir = Files.createTempDirectory(PointRangeBenchmark.class.getSimpleName());
        try (Directory directory = FSDirectory.open(tmpDir)) {
            Query[] queries;
            try (BufferedReader bufferedReader = openInputFile(Path.of(args[0]))) {
                try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                    int numDocs = Integer.parseInt(bufferedReader.readLine());
                    for (int i = 0; i < numDocs; i++) {
                        writer.addDocument(Collections.singleton(new LongPoint(TIMESTAMP_FIELD,
                                Long.parseLong(bufferedReader.readLine()) + GenerateNumericDataPoints.BASE_TIMESTAMP)));
                        if (i > 0 && i % 1_000_000 == 0) {
                            writer.flush();
                        }
                    }
                }
                int numQueries = Integer.parseInt(bufferedReader.readLine());
                queries = new Query[numQueries];
                for (int i = 0; i < numQueries; i++) {
                    String line = bufferedReader.readLine();
                    int commaPos = line.indexOf(',');
                    long start = Long.parseLong(line.substring(0, commaPos)) + GenerateNumericDataPoints.BASE_TIMESTAMP;
                    long end = Long.parseLong(line.substring(commaPos + 1)) + GenerateNumericDataPoints.BASE_TIMESTAMP;
                    queries[i] = LongPoint.newRangeQuery(TIMESTAMP_FIELD, start, end);
                }
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Do 10,000 queries to warm up
                int warmupCount = Math.min(10000, queries.length);
                for (int i = 0; i < warmupCount; i++) {
                    searcher.search(queries[i], 10);
                }
                int[] finalQps = new int[1];
                try (ExecutorService executor = Executors.newFixedThreadPool(workerCount)) {
                    AtomicInteger pool = new AtomicInteger(0);
                    AtomicBoolean stop = new AtomicBoolean(false);
                    int queryOffset = queries.length / workerCount;
                    for (int i = 0; i < workerCount; i++) {
                        executor.execute(new Worker(queries, searcher, queryOffset * i, pool, stop));
                    }
                    CountDownLatch stopLatch = new CountDownLatch(1);
                    try (ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
                        scheduler.scheduleAtFixedRate(new PoolFiller(finalQps, pool, stopLatch, 5000, 500), 0, 1, TimeUnit.SECONDS);
                        stopLatch.await();
                        stop.set(true); // Terminate the workers
                    }
                }
                System.out.println("Final QPS: " + finalQps[0]);
            }
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
        }
    }
}
