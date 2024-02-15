package example.benchmarks;

import example.basic.SimpleSearch;
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
import java.util.zip.GZIPInputStream;

public class PointRangeBenchmark {

    private static final String TIMESTAMP_FIELD = "timestamp";

    private static BufferedReader openInputFile(Path inputPath) throws IOException {
        InputStream inputStream = Files.newInputStream(inputPath);
        if (inputPath.toString().endsWith(".gz")) {
            inputStream = new GZIPInputStream(inputStream);
        }
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws IOException {
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
            long sum = 0;
            long min = Long.MAX_VALUE;
            long max = 0;
            int[] timeBuckets = new int[32];
            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Do 10,000 queries to warm up
                int warmupCount = Math.min(10000, queries.length);
                for (int i = 0; i < warmupCount; i++) {
                    searcher.search(queries[i], 10);
                }

                for (int i = 0; i < queries.length; i++) {
                    TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(10, Integer.MAX_VALUE);
                    long start = System.nanoTime();
                    searcher.search(queries[i], manager);
                    long time = System.nanoTime() - start;
                    int bucket = 63 - Long.numberOfLeadingZeros(time);
                    timeBuckets[bucket]++;
                    sum += time;
                    min = Math.min(min, time);
                    max = Math.max(max, time);
                    if (i > 0 && i % 5_000 == 0) {
//                        System.gc();
                        System.out.println("Average so far: " + sum / i);
                    }
                }
            }
            for (int i = 0; i < timeBuckets.length; i++) {
                if (timeBuckets[i] > 0) {
                    System.out.println((1 << i) + " - " + timeBuckets[i]);
                }
            }
            System.out.println("Average time: " + sum / queries.length);
        } finally {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
        }
    }


}
