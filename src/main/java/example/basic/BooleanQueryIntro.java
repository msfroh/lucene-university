// # Boolean query introduction
//
// While term queries are the basic build block for searching indexed text, Boolean queries are the most common way
// to combine multiple smaller queries to implement more complex matching logic. Though the implementing class is
// called `BooleanQuery`, they don't implement true Boolean logical operators (binary AND, binary OR, unary NOT).
// Instead, Lucene's Boolean queries use four unary operators:
//
// 1. `MUST`: Indicates that a clause must match for the Boolean query to be satisfied. The similarity score of the
//    match in the document is counted toward the document's score. Usually applied to clauses derived from user input,
//    `MUST` clauses say "This is what I'm looking for in a document".
// 2. `SHOULD`: Indicates that a match on the clause is optional, but if a document matches then the match counts
//    toward the document's score. If a query contains `SHOULD` clauses, but no `MUST` or `FILTER` clauses, then at
//    least one `SHOULD` clause must match (i.e. they are effectively ORed together). You can set the
//    `minimumNumberShouldMatch` property to indicate that some number of `SHOULD` clauses must be satisfied to get a
//    match. In the general case, `SHOULD` indicates that a clause is nice to have, but not a requirement.
// 3. `FILTER`: Similar to `MUST`, any `FILTER` clause must match, but a matching `FILTER` clause will not be counted
//    toward the document's score. These clauses are added to restrict the result set without saying anything about
//    the value of the matching clause. Examples include restricting documents by a date range, or limiting to
//    documents with the color red, etc.
// 4. `MUST_NOT`: Excludes documents that match the given clause. Like `FILTER`, these clauses do not impact document
//    scores, but change the returned results.
//
//
package example.basic;

public class BooleanQueryIntro {
}
