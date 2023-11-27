// # Analyzer basics
//
// This example will cover the basics of how Lucene converts text into a stream of "terms" using Analyzers.
//
package example.basic;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;

public class AnalyzerBasics {

    public static void main(String[] args) throws IOException {
        // ## Standard Analyzer
        //
        // In the previous examples, we created an `IndexWriter` by passing in `new IndexWriterConfig()`.
        // The no-args `IndexWriterConfig()` constructor delegates to the `IndexWriterConfig(Analyzer)` constructor,
        // passing `new StandardAnalyzer()`. So, in those examples, we implicitly used `StandardAnalyzer` to turn text
        // into a stream of tokens.
        //
        try (StandardAnalyzer standardAnalyzer = new StandardAnalyzer()) {

            // The analyzer for a field is made up of a `Tokenizer` followed by zero or more `TokenFilter` instances.
            // We get these components via one of the `tokenStream` methods -- one takes a `String`, while the other
            // takes a `Reader`. The `Reader` implementation streams tokens without loading the full string into memory,
            // but the `String` implementation is fine for our simple examples.
            //
            // An analyzer may configure different components per field, but StandardAnalyzer creates the same components
            // for any field. The field name `text` here could have been anything.
            //
            // StandardAnalyzer is made up of a StandardTokenizer, followed by a LowerCaseFilter, followed by a StopFilter.
            // Since we didn't pass any stop words to the StandardAnalyzer constructor, the StopFilter doesn't remove any
            // tokens.
            //
            try (TokenStream tokenStream = standardAnalyzer.tokenStream("text",
                    "The quick fox jumped over the lazy, brown dog")) {

                // A token stream reuses as much as possible across inputs and across tokens. Instead of returning a new object
                // for each token, the stream updates "attributes" associated with each token. To read the attribute values,
                // we need a reference to each available attribute.
                //
                // The following attributes get set by StandardTokenizer. The CharTermAttribute is modified by LowerCaseFilter.
                //
                CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncAttribute = tokenStream.getAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offsetAttribute = tokenStream.getAttribute(OffsetAttribute.class);
                TypeAttribute typeAttribute = tokenStream.getAttribute(TypeAttribute.class);

                // Before you can read tokens off the token stream, you must `reset` it.
                //
                tokenStream.reset();

                // Each call to `incrementToken` advances the stream to the next token, updating the internal values of each
                // of the attributes.
                //
                // This block outputs:
                // ```
                // Term: the, posInc: 1, startOffset: 0, endOffset: 3, type: <ALPHANUM>
                // Term: quick, posInc: 1, startOffset: 4, endOffset: 9, type: <ALPHANUM>
                // Term: fox, posInc: 1, startOffset: 10, endOffset: 13, type: <ALPHANUM>
                // Term: jumped, posInc: 1, startOffset: 14, endOffset: 20, type: <ALPHANUM>
                // Term: over, posInc: 1, startOffset: 21, endOffset: 25, type: <ALPHANUM>
                // Term: the, posInc: 1, startOffset: 26, endOffset: 29, type: <ALPHANUM>
                // Term: lazy, posInc: 1, startOffset: 30, endOffset: 34, type: <ALPHANUM>
                // Term: brown, posInc: 1, startOffset: 36, endOffset: 41, type: <ALPHANUM>
                // Term: dog, posInc: 1, startOffset: 42, endOffset: 45, type: <ALPHANUM>
                // ```
                //
                // The tokenizer splits on punctuation and spaces. Each word has a "position increment" of 1, meaning
                // that it directly follows the preceding word, useful for phrase queries. The offsets may be stored
                // in order to highlight matching words. All of these terms are of type ALPHANUM. We'll produce tokens
                // of other types below.
                //
                while (tokenStream.incrementToken()) {
                    System.out.println("Term: " + new String(charTermAttribute.buffer(), 0, charTermAttribute.length()) +
                            ", posInc: " + posIncAttribute.getPositionIncrement() +
                            ", startOffset: " + offsetAttribute.startOffset() +
                            ", endOffset: " + offsetAttribute.endOffset() +
                            ", type: " + typeAttribute.type());
                }
            }
        }

        // ## Stop words
        //
        // Let's repeat the above example, but this time, we're going to exclude the word "the" as a stop word.
        // We can do that by passing it to the `StandardAnalyzer` constructor.
        //
        CharArraySet stopWords = new CharArraySet(1, true);
        stopWords.add("the");
        try (StandardAnalyzer standardAnalyzer = new StandardAnalyzer(stopWords)) {
            // Let's make the answer a little more interesting by adding some new token types.
            //
            try (TokenStream tokenStream = standardAnalyzer.tokenStream("text",
                    "The quick fox jumped over the lazy, brown dog. 867-5309. マイクルと言います ☺")) {

                // This block outputs:
                // ```
                // Term: quick, posInc: 2, startOffset: 4, endOffset: 9, type: <ALPHANUM>
                // Term: fox, posInc: 1, startOffset: 10, endOffset: 13, type: <ALPHANUM>
                // Term: jumped, posInc: 1, startOffset: 14, endOffset: 20, type: <ALPHANUM>
                // Term: over, posInc: 1, startOffset: 21, endOffset: 25, type: <ALPHANUM>
                // Term: lazy, posInc: 2, startOffset: 30, endOffset: 34, type: <ALPHANUM>
                // Term: brown, posInc: 1, startOffset: 36, endOffset: 41, type: <ALPHANUM>
                // Term: dog, posInc: 1, startOffset: 42, endOffset: 45, type: <ALPHANUM>
                // Term: 867, posInc: 1, startOffset: 47, endOffset: 50, type: <NUM>
                // Term: 5309, posInc: 1, startOffset: 51, endOffset: 55, type: <NUM>
                // Term: マイクル, posInc: 1, startOffset: 57, endOffset: 61, type: <KATAKANA>
                // Term: と, posInc: 1, startOffset: 61, endOffset: 62, type: <HIRAGANA>
                // Term: 言, posInc: 1, startOffset: 62, endOffset: 63, type: <IDEOGRAPHIC>
                // Term: い, posInc: 1, startOffset: 63, endOffset: 64, type: <HIRAGANA>
                // Term: ま, posInc: 1, startOffset: 64, endOffset: 65, type: <HIRAGANA>
                // Term: す, posInc: 1, startOffset: 65, endOffset: 66, type: <HIRAGANA>
                // Term: ☺, posInc: 1, startOffset: 67, endOffset: 68, type: <EMOJI>
                // ```

                CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncAttribute = tokenStream.getAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offsetAttribute = tokenStream.getAttribute(OffsetAttribute.class);
                TypeAttribute typeAttribute = tokenStream.getAttribute(TypeAttribute.class);

                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    System.out.println("Term: " + new String(charTermAttribute.buffer(), 0, charTermAttribute.length()) +
                            ", posInc: " + posIncAttribute.getPositionIncrement() +
                            ", startOffset: " + offsetAttribute.startOffset() +
                            ", endOffset: " + offsetAttribute.endOffset() +
                            ", type: " + typeAttribute.type());
                }
            }
        }
    }
}
