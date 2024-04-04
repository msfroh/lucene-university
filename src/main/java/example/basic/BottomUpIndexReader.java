// # Bottom-up explanation of how Lucene reads index data
//
// This example will more or less bridge the gap between DirectoryFileContents (which looks at the Lucene files on disk)
// and SearchWithTermsEnum (which looks at how a TermQuery runs against an IndexReader, the layer below IndexSearcher).
//
// The example will cover the following:
// * **Directory**: Models a (platform-independent) file system directory, with simple operations like listing available
//   files, deleting files, and opening files for read/write.
// * **IndexInput / IndexOutput**: Abstraction for a file reading/writing, returned from a Directory. IndexInput
//   supports "slicing", where you can get an IndexInput corresponding to a byte range from another IndexInput. Note
//   that IndexInput and IndexOutput also provide (via inheritance from DataInput/DataOutput) methods to read/write
//   frequently-used primitives as bytes (e.g. VInt, ZInt, string maps, arrays of numeric types).
// * **Codecs**: Codecs are the (Lucene version-specific) accumulation of readers/writers for various data structures
//   used by the higher-level query logic. Each data structure tends to have an associated "[Version][Type]Format"
//   class that either directly implements reading/writing or can return readers and writers.
// * **Tying it together**: Many Lucene data structures are stored off-heap, which is achieved by making the reader
//   implement the data structure's interface, so navigating the data structure makes the (Codec-specific) reader move
//   through the IndexInput, which is usually a memory-mapped file supplied by MMapDirectory.
//
package example.basic;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BottomUpIndexReader {
    // ## Index setup
    //
    // We'll create a similar index to DirectoryFileContents, but we'll stick with the default codec, since we're
    // going to explore how the codec is used to decode the index files (since codec is short for "coder and decoder").
    //
    private static Path createLuceneIndex() throws IOException {
        Path indexDir = Files.createTempDirectory(DirectoryFileContents.class.getSimpleName());

        try (FSDirectory directory = FSDirectory.open(indexDir);
             IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
            List<String> texts = List.of(
                    "The quick fox jumped over the lazy, brown dog",
                    "Lorem ipsum, dolor sit amet",
                    "She left the web, she left the loom, she made three paces through the room",
                    "The sly fox sneaks past the oblivious dog"
            );
            int i = 0;
            for (String text : texts) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new TextField("text", text, Field.Store.YES));
                doc.add(new IntField("val", i++, Field.Store.YES));
                indexWriter.addDocument(doc);
            }
        }
        return indexDir;
    }

    // ## Reading the directory contents
    //
    public static void main(String[] args) throws IOException {
        Path indexDir = createLuceneIndex();
        Lock lock = null;
        // The Lucene `Directory` class provides an abstraction matching a filesystem directory (with no
        // subdirectories). In our case, the Lucene directory really is a filesystem directory corresponding to the
        // path of `indexDir`.
        //
        // Note that on most 64-bit Java runtime environments, `FSDirectory.open(path)` just calls
        // `new MMapDirectory(path)`. Using `FSDirectory.open` is the more portable choice, though.
        try (Directory dir = FSDirectory.open(indexDir)) {
            // The following snippet outputs the files in the directory and their sizes:
            //
            // ```
            // Directory contents:
            // _0.cfe   size = 479
            // _0.cfs   size = 2337
            // _0.si   size = 348
            // segments_1   size = 154
            // write.lock   size = 0
            // ```
            System.out.println("Directory contents:");
            for (String file : dir.listAll()) {
                System.out.println(" " + file + "   size = " + dir.fileLength(file));
            }

            // The `write.lock` file is there to make sure that only one `IndexWriter` can operate on the directory
            // at a time. For fun, let's lock the directory, to make extra sure that another `IndexWriter` can't mess
            // with our files. We'll release it in the `finally` block.
            lock = dir.obtainLock("write.lock");

            // The next file of interest is `segments_1`. An `IndexWriter` or `IndexReader` looks at these `segments_N`
            // files to determine the current "commit generation" and understand what segments are in the index.
            // That is, each time we commit, this number is incremented by 1 and written (in base 36, so 0-9a-z) as a
            // suffix to this "segment infos" file. We're currently on generation 1.
            System.out.println("Commit generation is " + SegmentInfos.getLastCommitGeneration(dir.listAll()));

            // Unlike most Lucene files, the segment infos file needs a relatively stable format to be understood by
            // different versions of Lucene. Instead of digging into the specific bytes in this file, we'll use
            // `SegmentInfos.readCommit` to load it and call some methods to output its contents. Note that the
            // `SegmentInfos` instance maintains a list of `SegmentCommitInfo`.
            //
            // ```
            // Segment _0 has the following files:
            // _0.cfe
            // _0.si
            // _0.cfs
            // ```
            SegmentInfos segmentInfos = SegmentInfos.readCommit(dir, "segments_1");
            for (SegmentCommitInfo sci : segmentInfos) {
                SegmentInfo segmentInfo = sci.info;
                System.out.println("Segment " + segmentInfo.name + " has the following files:");
                for (String segmentfile : sci.files()) {
                    System.out.println(segmentfile);
                }
            }
            // ### What are these SegmentCommitInfos?
            //
            // Since Lucene segments are written once, documents are never "deleted" from a segment. Instead, a
            // subsequent commit writes another file (with a suffix with the commit generation and a .liv extension,
            // like `_0_9.liv`), which is a bit set indicating what documents are still "live" (i.e. not deleted).
            // A `SegmentCommitInfo` holds a reference to the (written-once) `SegmentInfo` and a reference to the live
            // docs for the current commit. If the index uses updatable doc values, it will also reference (per-commit)
            // doc value updates.


            // ### Reading a single segment
            //
            // The files in each segment start with an underscore and the segment generation (also a base 36 counter,
            // but separate from the commit generation). The file extensions indicate what they contain. In this case,
            // the `_0.si` file contains the `SegmentInfo` for the segment. We already loaded this file when we called
            // `SegmentInfos.readCommit`. Among other things, the `SegmentInfo` knows what `Codec` to use to read the
            // other files in the segment. (Technically, the `Codec` class name is stored in the `segments_1` file,
            // since`SegmentInfos.readCommit()` uses the `Codec` to read the `.si` file.)
            //
            // At the time of writing (and subject to change with new versions), this outputs:
            //
            // ```
            // Segment _0 was written with the Lucene99 codec
            // ```

            SegmentCommitInfo seg0commitInfo = segmentInfos.info(0);
            SegmentInfo seg0info = seg0commitInfo.info;
            Codec codec = seg0info.getCodec();
            System.out.println("Segment _0 was written with the " + codec.getName() + " codec.");

            // ### Compound file segments
            //
            // Since our single segment is small, Lucene chose to stick all the individual files into the file `_0.cfs`,
            // to avoid consuming many operating system file handles for a bunch of even smaller files. (It's not a
            // problem with a single small segment, but can become a problem with many small segments or many small
            // Lucene indices open on the same machine.) The `_0.cfe` file holds the "compound file entries", which
            // store the start and end offsets for each file stored within the `.cfs` file.
            //
            // The compound file segment is modeled as a directory itself.
            //
            // ```
            // Compound file segment _0.cfs has the following contents:
            // _0_Lucene99_0.tip
            // _0.nvm
            // _0.fnm
            // _0_Lucene99_0.doc
            // _0_Lucene99_0.tim
            // _0_Lucene99_0.pos
            // _0_Lucene90_0.dvd
            // _0.kdd
            // _0.kdm
            // _0_Lucene99_0.tmd
            // _0_Lucene90_0.dvm
            // _0.kdi
            // _0.fdm
            // _0.nvd
            // _0.fdx
            // _0.fdt
            // ```
            CompoundDirectory seg0dir = codec.compoundFormat().getCompoundReader(dir, seg0info, IOContext.DEFAULT);
            System.out.println("Compound file segment _0.cfs has the following contents:");
            for (String innerFile : seg0dir.listAll()) {
                System.out.println(" " + innerFile);
            }

            // ### Compound file internals
            //
            // Let's decode that `.cfe` file ourselves.
            //
            // DON'T TRY THIS AT HOME. (Or at least don't ever do this in a real application.)
            //
            // Reading the Lucene90CompoundFormat source code, we see that the `.cfe` file has the following format:
            // ```
            // <header: validates file integrity>
            // <numfiles: VInt>
            // <file 0 suffix: string> <file 0 start offset: long> <file 0 end offset: long>
            // <file 1 suffix: string> <file 1 start offset: long> <file 1 end offset: long>
            // ...
            // <file N suffix: string> <file N start offset: long> <file N end offset: long>
            // <footer: validates file integrity>
            // ```
            //
            // This assumes we wrote the index using the Lucene90CompoundFormat. If it doesn't work for you,
            // you can delete or comment out this code. The higher-level APIs should continue to compile and work,
            // but low-level details are subject to changes.
            //
            // ```
            // File with suffix .nvd starts at offset 48 and has length 63
            // File with suffix .fdx starts at offset 112 and has length 64
            // File with suffix .kdi starts at offset 176 and has length 68
            // File with suffix _Lucene99_0.tip starts at offset 248 and has length 72
            // File with suffix _Lucene90_0.dvd starts at offset 320 and has length 74
            // File with suffix .kdd starts at offset 400 and has length 82
            // File with suffix _Lucene99_0.doc starts at offset 488 and has length 87
            // File with suffix .nvm starts at offset 576 and has length 103
            // File with suffix _Lucene99_0.pos starts at offset 680 and has length 114
            // File with suffix .kdm starts at offset 800 and has length 135
            // File with suffix .fdm starts at offset 936 and has length 157
            // File with suffix _Lucene90_0.dvm starts at offset 1096 and has length 162
            // File with suffix _Lucene99_0.tmd starts at offset 1264 and has length 190
            // File with suffix .fnm starts at offset 1456 and has length 250
            // File with suffix _Lucene99_0.tim starts at offset 1712 and has length 290
            // File with suffix .fdt starts at offset 2008 and has length 313
            // These should be equal: 479 == 479
            // ```
            try (ChecksumIndexInput cfeInput = dir.openChecksumInput("_0.cfe")) {
                /* If/when Lucene changes this file format, this header check should fail. */
                CodecUtil.checkIndexHeader(cfeInput, "Lucene90CompoundEntries", 0, 0, seg0info.getId(), "");
                int numFiles = cfeInput.readVInt();
                for (int i = 0 ; i < numFiles; i++) {
                    String fileSuffix = cfeInput.readString();
                    long start = cfeInput.readLong();
                    long end = cfeInput.readLong();
                    System.out.printf("File with suffix %s starts at offset %d and has length %d%n", fileSuffix, start, end);
                }
                CodecUtil.checkFooter(cfeInput);
                /* Check that we actually reached the end of the file */
                System.out.println("These should be equal: " + cfeInput.getFilePointer() + " == " + dir.fileLength("_0.cfe"));
            }

            // ### Reading terms from the index
            //
            // We've previously covered term queries from a high level in SimpleSearch, then a slightly lower level in
            // SearchWithTermsEnum. Let's close the gap by reading the terms directly from this segment (essentially
            // what the LeafReader did for us in SearchWithTermsEnum).
            //
            // First we need the information about the fields in the index, available via FieldInfos, which we read from
            // the `.fnm` file in our compound file segment. Then we get a `FieldsProducer` using the codec's postings
            // format. That lets us load the `Terms` (and therefore `TermsEnum`) for a given field.
            //
            // The code outputs the following terms:
            //
            // ```
            // The field 'text' has the following terms:
            // amet brown dog dolor fox ipsum jumped lazy left loom lorem made oblivious over paces past quick room
            // she sit sly sneaks the three through web
            // ```
            FieldInfos fieldInfos = codec.fieldInfosFormat().read(seg0dir, seg0info, "", IOContext.DEFAULT);
            SegmentReadState segmentReadState = new SegmentReadState(seg0dir, seg0info, fieldInfos, IOContext.DEFAULT);
            try (FieldsProducer fieldsProducer = codec.postingsFormat().fieldsProducer(segmentReadState)) {
                Terms terms = fieldsProducer.terms("text");
                TermsEnum termsEnum = terms.iterator();
                BytesRef termBytesRef;
                System.out.println("The field 'text' has the following terms:");
                StringBuilder builder = new StringBuilder();
                int lineLength = 0;
                while ((termBytesRef = termsEnum.next()) != null) {
                    String termString = termBytesRef.utf8ToString();
                    builder.append(termString).append(' ');
                    lineLength += termString.length();
                    if (lineLength > 80) {
                        builder.append('\n');
                        lineLength = 0;
                    }
                }
                System.out.println(builder);
            }
            // ### Memory-mapped files
            //
            // In the above example, we initialized a `FieldsProducer`, which first opened an `IndexInput` "slice" of the
            // `_0.cfs` `IndexInput` corresponding to the `_0_Lucene99_0.doc` file (for the postings -- the doc ids for
            // each term). Then it opened another `IndexInput` slice in the CFS file for `_0_Lucene99_0.tim` (the terms),
            // as well as slices for `_0_Lucene99_0.tip` (the terms index), and `_0_Lucene99_0.tmd` (the term metadata).
            //
            // For each indexed field in the index (in this case, just the `text` field), the postings reader
            // (technically the Lucene90BlockTreeTermsReader) allocated a `FieldReader`. Each `FieldReader` holds some
            // field metadata (e.g. first/last term, sum of doc frequencies and term frequencies across all terms for
            // the field), but otherwise it's a lazy data structure that reads from the files opened above when asked
            // (but not the`.tmd` file -- we're done with that) when asked. It's also the `Terms` variable that we
            // retrieved above.
            //
            // Similarly, each time we call `termsEnum.next()`, we're just advancing a pointer into a memory-mapped
            // file. The file is probably fully paged into memory (since the whole `_0.cfs` file is only 2337
            // bytes), so it's really not much worse than reading values out of objects in the JVM heap.
        } finally {
            for (String indexFile : FSDirectory.listAll(indexDir)) {
                Files.deleteIfExists(indexDir.resolve(indexFile));
            }
            Files.delete(indexDir);
            if (lock != null) {
                lock.close();
            }
        }


    }

}
