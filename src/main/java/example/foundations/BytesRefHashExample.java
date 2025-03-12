// # BytesRefHash example
//
// Building on the previous example in `PrimitivesRef`, which introduced the Lucene `BytesRef` class, this example
// dives into `BytesRefHash`, a highly-optimized data structure for storing and sorting distinct `BytesRef` instances.
// A `BytesRefHash` operates like a `HashSet`
//
// The internal structure of `BytesRefHash` is fairly simple. It includes:
//
// * `count`: an `int` that serves as an auto-incrementing ID generator (starting at zero) and counter for the number of elements,
// * `ids`: an `int[]` acting as a hash map from values to their auto-incrementing IDs, with `-1` indicating an empty slot,
// * `byteStart`: an `int[]` array from IDs to starting byte offsets in a large byte buffer, and
// * `pool` : a large, chunked byte buffer that holds the actual `BytesRef` values, with their length encoded as a prefix.
//
// The basic flow for inserting into a `BytesRefHash` (within the `add(BytesRef)` method) is as follows:
//
// 1. Compute the hash code of the input `BytesRef`, modulo the size of `ids`, as a possible insert position.
//     1. If there is an entry at the given position in `ids`, check if it is the current `BytesRef`. The check works
//        by comparing against the value in `pool` starting at `byteStart[ids[position]]`.
//     2. If the existing entry **is not** the current `BytesRef`, then check the next position.
//     3. Keep checking consecutive positions until you either find the value being inserted or an empty slot.
// 2. Check the value of `ids` at the returned position.
//     1. If it's not `-1`, then we must already have the current value. Nothing to do. Return
//     2. Otherwise, we need to insert the value. Continue.
// 3. Write the current value of `counter` to `ids[position]`. This maps from the hash bucket to the numeric ID.
// 4. Append the new value to `pool`, saving the start offset in `byteStart[counter]`.
// 5. Increment `counter` in preparation for the next value.
//
//
package example.foundations;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.UnicodeUtil;

public class BytesRefHashExample {
    public static void main(String[] args) {
        BytesRefHash bytesRefHash = new BytesRefHash();
        // ## Insertion into a BytesRefHash
        //
        // The basic flow for inserting into a `BytesRefHash` (within the `add(BytesRef)` method) is as follows:
        //
        // 1. Compute the hash code of the input `BytesRef`, modulo the size of `ids`, as a possible insert position.
        //     1. If there is an entry at the given position in `ids`, check if it is the current `BytesRef`. The check works
        //        by comparing against the value in `pool` starting at `byteStart[ids[position]]`.
        //     2. If the existing entry **is not** the current `BytesRef`, then check the next position.
        //     3. Keep checking consecutive positions until you either find the value being inserted or an empty slot.
        // 2. Check the value of `ids` at the returned position.
        //     1. If it's not `-1`, then we must already have the current value. Nothing to do. Return
        //     2. Otherwise, we need to insert the value. Continue.
        // 3. Write the current value of `counter` to `ids[position]`. This maps from the hash bucket to the numeric ID.
        // 4. Append the new value to `pool`, saving the start offset in `byteStart[counter]`.
        // 5. Increment `counter` in preparation for the next value.

        BytesRef bytesRef = new BytesRef("foo");
        int id = bytesRefHash.add(bytesRef);
        // Position will be greater than zero, because we inserted a new value.
        assert id >= 0;
        setBytesRefValue(bytesRef, "bar");
        // Position will be greater than zero, because we inserted a new value.
        id = bytesRefHash.add(bytesRef);
        assert id >= 0;
        setBytesRefValue(bytesRef, "foo");
        id = bytesRefHash.add(bytesRef);
        // Position will be negative, because we've seen this value before.
        assert id < 0;

        // Let's add a bunch of values to the `BytesRefHash`:
        for (String word : "One point to be observed in the nature and history of words is their tendency to contract in form and degenerate in meaning.".split(" ")) {
            setBytesRefValue(bytesRef, word);
            bytesRefHash.add(bytesRef);
        }

        // ## Reading values
        //
        // The values in a `BytesRefHash` are assigned IDs sequentially from 0 (inclusive) to `bytesRefHash.size()` (exclusive).
        // Since a `BytesRef` is intended to be reused, we can use the same `BytesRef` instance to access values from
        // a `BytesRefHash`. Note, though, that the `BytesRef` is updated to point into the backing `pool` of the `BytesRefHash`,
        // so we must not modify the returned `bytes` array.
        //
        // The `get()` method does not do a hash table lookup, as it only needs to know the starting offset for each
        // value, which it gets from the internal `byteStart` array.
        //
        // The following will return values in the order that they were added to the `BytesRefHash` (without duplicates).
        System.out.println("---- Insertion-ordered values ----");
        for (int i = 0; i < bytesRefHash.size(); ++i) {
            bytesRefHash.get(i, bytesRef);
            System.out.println(i + " -> " + bytesRef.utf8ToString());
        }
        System.out.println();

        // ## Destructive operations
        //
        // There are two "destructive" operations on `BytesRefHash`, so named because they discard the structure of the
        // `ids` hash table.
        //
        // The first is `compact()`, which moves all `ids` values to the beginning of the table and
        // returns them. Note that the returned array is the size of the full hash table, with the `-1` entries pushed
        // to the end, so we must take care to only iterate over the first `bytesRefHash.size()` entries.
        // The following outputs the set of inserted values in seemingly random (hash) order.
        System.out.println("---- Hash-ordered values ----");
        int[] ids = bytesRefHash.compact();
        for (int i = 0; i < bytesRefHash.size(); i++) {
            bytesRefHash.get(ids[i], bytesRef);
            System.out.println(ids[i] + " -> " + bytesRef.utf8ToString());
        }
        System.out.println();
        // The `compact()` method on its own is not used anywhere in Lucene outside of the `sort()` method, described
        // next. The `compact()` method should probably be made `private`.

        // The second destructive operation is `sort()`. Part of why it is destructive is that it relies on compact().
        // The output of `sort()` is also an array of ids, but instead of appearing in hash order, they are in lexicographic
        // order of the corresponding values.
        System.out.println("---- Sorted values ----");
        int[] sortedIds = bytesRefHash.sort();
        for (int i = 0; i < bytesRefHash.size(); i++) {
            bytesRefHash.get(sortedIds[i], bytesRef);
            System.out.println(sortedIds[i] + " -> " + bytesRef.utf8ToString());
        }

        // The `sort()` method is used to produce a sorted set of unique values. This is how Lucene builds
        // `SortedSetDocValues` (via `SortedSetDocValuesWriter`), as well as the term dictionary used during
        // an index writer flush (via `TermsHashPerField` and its subclasses). This deduplicating and sorting
        // behavior is key to the construction of a search index.
    }

    // The following is a helper function to load string values into a (reusable) `BytesRef` instance.
    private static void setBytesRefValue(BytesRef bytesRef, String value) {
        bytesRef.bytes = ArrayUtil.growNoCopy(bytesRef.bytes, UnicodeUtil.maxUTF8Length(value.length()));
        bytesRef.offset = 0;
        bytesRef.length = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), bytesRef.bytes);
    }
}
