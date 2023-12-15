// # Lucene file content (with SimpleTextCodec)
//
// This worked example helps explain what data structures exist in a Lucene index and how they are stored (albeit with
// a text representation -- the real implementations use very compact binary files).
//
// Unlike some other examples, the interesting part is not the code, but rather the output, which is included below
// the `DirectoryFileContents` class.
//
// There are a number of changes that can be made to this example to make it more interesting (at the expense of
// producing files too large to walk through here). Readers are encouraged to modify the `createDocuments` method
// to add other field types, add more documents, etc.
//

package example.basic;

import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DirectoryFileContents {

    // ## Creating Documents
    //
    // We will mostly reuse the same documents from SimpleSearch, but we'll also add a numeric `IntField` which will
    // write both points and doc values. Points are covered in more detail by examples in the "points" package.
    //
    // To make the output mildly more interesting, let's not add the numeric field to one of the documents.
    private static List<List<IndexableField>> createDocuments() {
        List<String> texts = List.of(
                "The quick fox jumped over the lazy, brown dog",
                "Lorem ipsum, dolor sit amet",
                "She left the web, she left the loom, she made three paces through the room",
                "The sly fox sneaks past the oblivious dog"
        );
        List<List<IndexableField>> docs = new ArrayList<>();
        int i = 0;
        for (String text : texts) {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new TextField("text", text, Field.Store.YES));
            /* I want one document to miss "val" */
            if (++i != 2) {
                doc.add(new IntField("val", i, Field.Store.YES));
            }
            docs.add(doc);
        }
        return docs;
    }

    // ## Create the Lucene index with SimpleTextCodec
    //
    public static void main(String[] args) throws IOException {
        Path tmpDir = Files.createTempDirectory(DirectoryFileContents.class.getSimpleName());

        // In other examples, we have been using the default `IndexWriterConfig`. This time, we construct the
        // IndexWriterConfig, but override the codec.
        //
        // Codecs are Lucene's abstraction that define how low-level constructs are written as files. The default
        // codecs are highly-tuned for compact size and good read/write performance, while SimpleTextCodec is designed
        // to be human-readable. The JavaDoc for SimpleTextCodec (and its associated classes) says
        // **FOR RECREATIONAL USE ONLY**.
        //
        IndexWriterConfig conf = new IndexWriterConfig();
        conf.setCodec(new SimpleTextCodec());
        // By default,
        conf.setUseCompoundFile(false);
        try (Directory directory = FSDirectory.open(tmpDir);
             IndexWriter writer = new IndexWriter(directory, conf)) {
            for (List<IndexableField> doc : createDocuments()) {
                writer.addDocument(doc);
            }
        } finally {
            // ## Dump the index contents
            //
            // Once we've closed the IndexWriter, before we delete each file, we print each file name and its size.
            //
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Path path = tmpDir.resolve(indexFile);
                long size = Files.size(path);
                System.out.println("File " + indexFile + " has size " + size);
                // Don't output the segmentinfos (.si) file yet, because it may contain non-UTF-8 bytes.
                // (See [https://github.com/apache/lucene/pull/12897](https://github.com/apache/lucene/pull/12897).)
                // Also, don't output the "segments_1" file, because it is always a binary file, independent of the
                // codec. (The `IndexReader` opens the last "segments_*", which explains what codec was used to write
                // each segment.)
                if (!indexFile.endsWith(".si") && !indexFile.startsWith("segments_")) {
                    System.out.println(Files.readString(path));
                }
                Files.deleteIfExists(path);
            }
            // Then we delete the directory itself.
            Files.deleteIfExists(tmpDir);
        }
    }
}
/*
// ## Program output
//
// The program dumps the following files. Let's explore the files one-by-one.
//
// Note that the SimpleTextCodec is an implementation that is conceptually similar to the real binary codecs, but
// certainly // not identical. There are compromises that SimpleTextCodec has made to implement a fully-functioning
// codec in plain text.

// ### Doc values
//
// The `.dat` file stores the doc values for the `val` field.
//
// The `IntField` uses the "binary" doc values format. In this case, each value has a maximum length of 1 byte.
// The "maxlength" and "pattern" values let us efficiently seek to the start of a document's values. Specifically,
// relative to the start of the ddc values (i.e. the byte following the newline after `pattern 0`), a given document's
// values start at `startOffset + (9 + pattern.length + maxlength + 2) * docNum` (taken from the Javadoc for
// `SimpleTextDocValuesFormat`).
//
// Each document's entry has a `length`, specifying how many doc values are present in the document. In our case,
// each document has a single value for `val`, except the second document, which has none.
//
// Each Lucene file has a trailing `checksum` used to verify the file integrity and protect against flipped bits.
//
File _0.dat has size 136
field val
  type BINARY
  maxlength 1
  pattern 0
length 1
1
T
length 0

F
length 1
3
T
length 1
4
T
END
checksum 00000000001474172410

// ### Points
//
// The `.dii` file stores an index to locate the point data for individual fields in the `.dim` file. In this case,
// the point tree for the field `val` starts at byte 113 in the `.dim` file.
//
// That byte corresponds to the line `num data dims 1`. The contents of the file before that are the "blocks" in the
// "block K-d" tree, corresponding to leaves of the tree. In this case, since we only have 3 documents with points,
// they all fit in a single block. The offset of this leaf is specified as part of the tree definition, in the line
// `block fp 0` (i.e. the block starts at byte 0 of the file).
File _0.dii has size 79
field count 1
  field fp name val
  field fp 113
checksum 00000000001996750873

File _0.dim has size 361
block count 3
  doc 0
  doc 2
  doc 3
  block value [80 0 0 1]
  block value [80 0 0 3]
  block value [80 0 0 4]
num data dims 1
num index dims 1
bytes per dim 4
max leaf points 3
index count 1
min value [80 0 0 1]
max value [80 0 0 4]
point count 3
doc count 3
  block fp 0
split count 1
  split dim 0
  split value [0 0 0 0]
END
checksum 00000000000107327399

// ### Stored fields
//
// The `.fld` file keep stored field, used to retrieve the original field values as sent to the index writer.
//
// Stored fields are organized into a hierarchy of Document -> Field ordinal -> Field value. A multi-valued field
// (not to be confused with a multi-dimensional point) is just the same field added to the document multiple times,
// and will be assigned multiple stored field ordinals based on the order that the fields were added.
//
File _0.fld has size 593
doc 0
  field 0
    name text
    type string
    value The quick fox jumped over the lazy, brown dog
  field 1
    name val
    type int
    value 1
doc 1
  field 0
    name text
    type string
    value Lorem ipsum, dolor sit amet
doc 2
  field 0
    name text
    type string
    value She left the web, she left the loom, she made three paces through the room
  field 1
    name val
    type int
    value 3
doc 3
  field 0
    name text
    type string
    value The sly fox sneaks past the oblivious dog
  field 1
    name val
    type int
    value 4
END
checksum 00000000000213864262

// ### Field infos
//
// The `.inf` file stores information about each field.
//
// Note that many of the properties (e.g. `vector encoding` and `vector similarity`) are not applicable to the fields
// that we added. The values shown here are the field defaults. Several of the data structures are only created
// for a field if some property is set. For example, vectors are only written for fields where the
// "vector number of dimensions" is greater than zero. Doc values are only written when the doc values type for a field
// is not `NONE`. See the `IndexingChain.processField` method to see exactly how field type values decide what
// structures get written to an index for a field based on the field type properties.
//
File _0.inf has size 758
number of fields 2
  name text
  number 0
  index options DOCS_AND_FREQS_AND_POSITIONS
  term vectors false
  payloads false
  norms true
  doc values NONE
  doc values gen -1
  attributes 0
  data dimensional count 0
  index dimensional count 0
  dimensional num bytes 0
  vector number of dimensions 0
  vector encoding FLOAT32
  vector similarity EUCLIDEAN
  soft-deletes false
  name val
  number 1
  index options NONE
  term vectors false
  payloads false
  norms true
  doc values SORTED_NUMERIC
  doc values gen -1
  attributes 0
  data dimensional count 1
  index dimensional count 1
  dimensional num bytes 4
  vector number of dimensions 0
  vector encoding FLOAT32
  vector similarity EUCLIDEAN
  soft-deletes false
checksum 00000000000798287814

// ### Norms
//
// The `.len` file contains the norms for each text field in the index. The norms are the length (relative to term
// positions) of a text field in each document containing that field.
//
// In this case, we have a single text field called `text`. The norms are encoded as a "delta" from the shortest
// document in the segment. In this case, our shortest document is the second one with length 5 (represented as 00
// more than the `minvalue`). The longest document is the third one with length 15 (i.e. 10 more than `minValue`).
//
// Field length per document is an important value used in the tf-idf and BM25 scoring formulae.
//
File _0.len has size 106
field text
  type NUMERIC
  minvalue 5
  pattern 00
04
T
00
T
10
T
03
T
END
checksum 00000000003850040528

// ### Postings
//
// The postings, stored in `.pst` files, are the key data structure used for efficient text search in Lucene.
//
// Postings are organized from field to term to matching documents. In this case, since the `text` field was indexed
// with `DOCS_AND_FREQS_AND_POSITIONS` (see the "Field infos" section above), each document entry for a term encodes
// the frequency of the term in the document (used in scoring calculations), as well as the positions at which the
// term can be found in the document (used for phrase and span queries).
//
// While many of the terms in our example occur in a single position in a single document, look at the postings for
// the term `the`, which appears in 3 documents, in multiple positions for each.
//
File _0.pst has size 1508
field text
  term amet
    doc 1
      freq 1
      pos 4
  term brown
    doc 0
      freq 1
      pos 7
  term dog
    doc 0
      freq 1
      pos 8
    doc 3
      freq 1
      pos 7
  term dolor
    doc 1
      freq 1
      pos 2
  term fox
    doc 0
      freq 1
      pos 2
    doc 3
      freq 1
      pos 2
  term ipsum
    doc 1
      freq 1
      pos 1
  term jumped
    doc 0
      freq 1
      pos 3
  term lazy
    doc 0
      freq 1
      pos 6
  term left
    doc 2
      freq 2
      pos 1
      pos 5
  term loom
    doc 2
      freq 1
      pos 7
  term lorem
    doc 1
      freq 1
      pos 0
  term made
    doc 2
      freq 1
      pos 9
  term oblivious
    doc 3
      freq 1
      pos 6
  term over
    doc 0
      freq 1
      pos 4
  term paces
    doc 2
      freq 1
      pos 11
  term past
    doc 3
      freq 1
      pos 4
  term quick
    doc 0
      freq 1
      pos 1
  term room
    doc 2
      freq 1
      pos 14
  term she
    doc 2
      freq 3
      pos 0
      pos 4
      pos 8
  term sit
    doc 1
      freq 1
      pos 3
  term sly
    doc 3
      freq 1
      pos 1
  term sneaks
    doc 3
      freq 1
      pos 3
  term the
    doc 0
      freq 2
      pos 0
      pos 5
    doc 2
      freq 3
      pos 2
      pos 6
      pos 13
    doc 3
      freq 2
      pos 0
      pos 5
  term three
    doc 2
      freq 1
      pos 10
  term through
    doc 2
      freq 1
      pos 12
  term web
    doc 2
      freq 1
      pos 3
END
checksum 00000000001512782415

// ### Segment info
//
// The segment info file `.si` stores information about all the other files involved in a segment.
//
// While the segment info file is managed by the codec, the SimpleTextSegmentInfoFormat implementation currently
// outputs the raw bytes for the segment's unique ID, so it is not a valid UTF-8 encoding.
// See https://github.com/apache/lucene/pull/12897.
File _0.si has size 739

// ### Commit file
//
// Each commit increments the commit generation and writes a `segments_<generation>` file. When indexing from a single
// thread with regular commits, the commit generation will often match the ordinal of the last segment (since each
// counts up by one on each commit). If segments are flushed without committing or flushed from multiple threads,
// the segment numbers will usually be higher than the commit generation.
//
// The commit file holds the "SegmentInfos" (plural). It is not managed by the codec, since it encodes the information
// about what segments are part of the given commit and which codecs were used to write each segment. Since the
// file was not written by SimpleTextCodec, it is a binary file, so we don't output it here.
///
File segments_1 has size 156

// ### Write lock
//
// On creation of the `IndexWriter`, a `write.lock` file is created and locked. The lock implementation is configurable,
// but is usually based on a `java.nio.channels.FileLock`.
//
// The write lock ensures that no more than one IndexWriter is ever writing to the same directory.
File write.lock has size 0

 */
