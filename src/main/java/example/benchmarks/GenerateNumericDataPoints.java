package example.benchmarks;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

public class GenerateNumericDataPoints {
    private static final Calendar GREGORIAN_CALENDAR = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    static final long BASE_TIMESTAMP;
    static {
        GREGORIAN_CALENDAR.set(2023, GregorianCalendar.JANUARY, 1, 0, 0);
        BASE_TIMESTAMP = GREGORIAN_CALENDAR.getTimeInMillis();
    }


    public static void main(String[] args) throws IOException {
        int numDatapoints = Integer.parseInt(args[0]);
        int numQueries = Integer.parseInt(args[1]);
        PrintStream out = System.out;
        if (args.length > 2) {
            OutputStream outputStream = Files.newOutputStream(Path.of(args[2]));
            if (args[2].endsWith(".gz")) {
                outputStream = new GZIPOutputStream(outputStream);
            }
            out = new PrintStream(outputStream, true, StandardCharsets.UTF_8);
        }

        out.println(numDatapoints);
        Random random = new Random();
        long bound = 1000L * 60 * 60 * 24 * 365;
        for (int i = 0; i < numDatapoints;) {
            long val = random.nextLong(bound);
            if (acceptLong(random, val)) {
                out.println(val);
                i++;
            }
        }

        out.println(numQueries);
        for (int i = 0; i < numQueries; i++) {
            long start = random.nextLong(bound);
            int numPeriods = random.nextInt(29) + 1;
            long periodSize = random.nextBoolean() ? MILLIS_PER_HOUR : MILLIS_PER_DAY;
            long[] range = createRange(start, numPeriods, periodSize);
            out.println(range[0] + "," + range[1]);
        }
        if (out != System.out) {
            out.close();
        }
    }

    private static final long MILLIS_PER_HOUR = 1000L * 60 * 60;
    private static final long MILLIS_PER_DAY = MILLIS_PER_HOUR * 24;
    private static long[] createRange(long start, int numPeriods, long periodSize) {
        long periodOfYearStart = start / periodSize;
        long[] range = new long[2];
        range[0] =  periodOfYearStart * periodSize;
        range[1] = range[0] + numPeriods * periodSize;
        return range;
    }




    private static boolean acceptLong(Random random, long val) {
        GREGORIAN_CALENDAR.setTimeInMillis(val + BASE_TIMESTAMP);
        int dayOfWeek = GREGORIAN_CALENDAR.get(Calendar.DAY_OF_WEEK);

        if (dayOfWeek == GregorianCalendar.SATURDAY || dayOfWeek == GregorianCalendar.SUNDAY) {
            if (random.nextBoolean()) {
                return false;
            }
        }
        int hourOfDay = GREGORIAN_CALENDAR.get(Calendar.HOUR_OF_DAY);
        if (hourOfDay >= 1 && hourOfDay < 5) {
            return random.nextBoolean();
        }
        return true;
    }
}
