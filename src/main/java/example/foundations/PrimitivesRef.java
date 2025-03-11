// # BytesRef (and other array references)
//
// One of Lucene's performance advantages is that it is very cautious about allocating memory, striving to reuse buffers
// where possible. While Java makes management of short-lived objects pretty lightweight, it's even more lightweight
// to avoid allocating them altogether.
//
// To simplify the management of arrays used as buffers, Lucene provides some wrapper classes, like `BytesRef`,
// `CharsRef`, `IntsRef`, and `LongsRef`.
//
// This example will focus on `BytesRef`, which is the most widely used within the Lucene codebase.
package example.foundations;

import org.apache.lucene.util.ArrayUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class PrimitivesRef {


    // Lucene's `BytesRef` holds a reference to a byte array, a start offset into the byte array representing the
    // start of the value of interest, and the length of the value of interest. Since we aim to reuse the buffer,
    // the length of the value is often less than the length of the array, since the array may have previously
    // held a larger value.
    private static class BytesRef {
        public static final byte[] EMPTY_BYTES = new byte[0];
        public byte[] bytes;
        public int offset;
        public int length;

        public BytesRef() {
            bytes = EMPTY_BYTES;
        }
    }

    // Let's use `BytesRef` to simulate tokenizing a text file into lines, and then splitting those lines into
    // individual words. This is similar to the logic used to tokenize a text file.
    //
    // To start, let's create a helper method that creates a text file. The content is taken from "A Book About Words"
    // by G. F. Graham, published in 1869, copied from https://www.gutenberg.org/ebooks/55200.
    //
    // Note that by using `Files#newBufferedWriter`, we guarantee that the resulting file is encoded with the UTF-8
    // character set.
    private static Path createTextFile() throws IOException {
        Path file = Files.createTempFile("text", ".txt");
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            writer.append("""
                    What is meant by a Language? It is a collection of all the words,
                    phrases, grammatical forms, idioms, &c., which are used by one
                    people. It is the outward expression of the tendencies, turn of
                    mind, and habits of thought of some one nation, and the best
                    criterion of their intellect and feelings. If this explanation be
                    admitted, it will naturally follow that the connection between a
                    people and their language is so close, that the one may be judged
                    of by the other; and that the language is a lasting monument of the
                    nature and character of the people.
                    
                    Every language, then, has its genius; forms of words, idioms, and
                    turns of expression peculiar to itself; by which, independently of
                    other differences, one nation may be distinguished from another.
                    This condition may be produced by various causes; such as soil,
                    climate, conquest, immigration, &c. Out of the old Roman, or Latin,
                    there arose several modern languages of Europe; all known by the
                    generic name--Romance; viz. Italian, French, Provençal, Spanish,
                    and Portuguese. These may be called daughters of ancient Latin; and
                    the natives of all these countries down to the seventh century,
                    both spoke and wrote that language. But when the Scandinavian and
                    Germanic tribes invaded the West of Europe, the Latin was broken
                    up, and was succeeded by Italian, French, Spanish, &c. The Latin
                    now became gradually more and more corrupt, and was, at length, in
                    each of these countries, wholly remodelled.
                    
                    History has been called ‘the study of the law of change;’ i.e. the
                    process by which human affairs are transferred from one condition
                    to another. The history of a language has naturally a close
                    analogy with political history; the chief difference being that
                    the materials of the latter are facts, events, and institutions;
                    whilst the former treats of words, forms, and constructions. Now,
                    in the same way as a nation never stands still, but is continually
                    undergoing a silent--perhaps imperceptible--transformation, so
                    it is with its language. This is proved both by experience and
                    reason. We need hardly say that the English of the present time
                    differ widely from the English of the fourteenth century; and we
                    may be quite sure that the language of this country, two or three
                    centuries hence, will be very different from what it is at present.
                    It would be impossible for a nation either to improve or decay, and
                    for its language at the same time to remain stationary. The one
                    being a reflex of the other, they must stand or fall together.
                    """);
        }
        return file;
    }

    public static void main(String[] args) throws IOException {

        Path textFile = createTextFile();

        // We allocate an initially-empty `BytesRef` for each line. We will dynamically resize it to accommodate
        // the maximum size needed for a line. We also allocate a `BytesRef` that will provide a view over each word in
        // each line. Note that the variable memory used by this program is given by the maximum size of
        // `lineBytesRef.bytes`. Everything else operates within bytes of that same buffer.
        BytesRef lineBytesRef = new BytesRef();
        BytesRef wordBytesRef = new BytesRef();

        // We open a basic Java `InputStream`, so that we can process the input file byte-by-byte.
        try (InputStream is = Files.newInputStream(textFile)) {
            // The following loop will collect each line into `linesBytesRef`.
            int linePos = 0;
            int nextByte;
            while ((nextByte = is.read()) >= 0) {
                // Let's consider the case where we're not at the end of a line yet.
                if (nextByte != '\n') {
                    // If the current `lineBytesRef` is not large enough, we use Lucene's `ArrayUtil#grow` helper
                    // method, which will allocate an exponentially-larger array and copy the existing contents.
                    //
                    // If we did not want to preserve the existing contents, we could use `ArrayUtil.growNoCopy`.
                    if (linePos == lineBytesRef.bytes.length) {
                        lineBytesRef.bytes = ArrayUtil.grow(lineBytesRef.bytes, linePos + 1);
                    }
                    // Append the current character to the line.
                    lineBytesRef.bytes[linePos++] = (byte) (nextByte & 0xFF);
                } else {
                    // Finished the current line. We set the length to the current position.
                    lineBytesRef.length = linePos;

                    // In the following method call, `wordsBytesRef` is just used as a reusable scratch variable
                    // that lets us filter over a subset of `lineBytesRef`.
                    outputWords(lineBytesRef, wordBytesRef);
                    // Reset linePos in preparation for the next line.
                    linePos = 0;
                }
            }
        } finally {
            // Clean up the file when we're done.
            Files.deleteIfExists(textFile);
        }
    }

    // The following helper method uses `wordsBytesRef` as a window over `lineBytesRef`. They share the same underlying
    // byte array, but `wordsBytesRef` is updated to point to the starting offset and length of each word.
    private static void outputWords(BytesRef lineBytesRef, BytesRef wordBytesRef) {
        wordBytesRef.offset = lineBytesRef.offset;
        wordBytesRef.bytes = lineBytesRef.bytes;
        for (int i = lineBytesRef.offset; i < lineBytesRef.offset + lineBytesRef.length; ++i) {
            if (Character.isWhitespace(wordBytesRef.bytes[i])) {
                // Reached the end of a word. Update the length of the word, then output it.
                wordBytesRef.length = i - wordBytesRef.offset;
                outputWord(wordBytesRef);
                // Update the word offset in preparation for the next word.
                wordBytesRef.offset = i + 1;
            }
        }
        // Output the last word from each line.
        wordBytesRef.length = lineBytesRef.length + lineBytesRef.offset - wordBytesRef.offset;
        outputWord(wordBytesRef);
    }

    private static void outputWord(BytesRef bytesRef) {
        // Output each word character-by-character.
        //
        // Note that because the input above contains some UTF-8 multi-byte characters, we have three branches to
        // handle 1-, 2-, and 3-byte characters. This logic is lifted from Lucene's `UnicodeUtil#UTF8toUTF16` method,
        // which is conveniently called from `BytesRef#utf8ToString`. (I skipped the 4-byte branch, since it's
        // complicated and not needed for this specific example.)
        //
        // In this case, I didn't want to use `BytesRef#utf8ToString`, since the goal of this example is to avoid
        // allocating any unnecessary extra objects, including the `String` objects returned by that method.
        for (int i = bytesRef.offset; i < bytesRef.offset + bytesRef.length; ++i) {
            int b = bytesRef.bytes[i] & 0xff;
            if (b < 0xc0) {
                assert b < 0x80;
                System.out.print((char) b);
            } else if (b < 0xe0) {
                char c  = (char) (((b & 0x1f) << 6) + (bytesRef.bytes[++i] & 0x3f));
                System.out.print(c);
            } else if (b < 0xf0) {
                char c =
                        (char) (((b & 0xf) << 12) + ((bytesRef.bytes[i+1] & 0x3f) << 6) + (bytesRef.bytes[i + 2] & 0x3f));
                i += 2;
                System.out.print(c);
            } else {
                throw new IllegalArgumentException();
            }
        }
        // Add a newline between words.
        System.out.println();
    }
}
