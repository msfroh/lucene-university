# Self-contained Lucene examples

This repository contains some examples of [Apache Lucene](https://lucene.apache.org/) features with verbose explanations
as code comments written in Markdown.

The goal is to provide code samples that can be used a few ways:

1. Read the source code. The comments should make what's going on pretty clear.
2. Open a code sample in your IDE and step through it with a debugger. Follow along with the comments as you go. Make 
changes to the code and see what happens. (Some examples include suggested changes.)
3. Read the code and documentation as a web page generated with [Docco](https://ashkenas.com/docco/) over at 
https://msfroh.github.io/lucene-university/docs/SimpleSearch.html. (Go to the "Jump to..." box in the top-right to load 
other examples.) This should feel kind of like reading a book.

## Getting started

This repository currently depends on Lucene 10, which requires JDK 21 or higher.

You can clone the repository and build the examples with:

```
git clone https://github.com/msfroh/lucene-university.git
cd lucene-university
./gradlew build
```

Using IntelliJ, you can use "File -> New -> Project from Existing Sources..." and point it to the location where the
code was cloned. Select "Import Project from Existing Model" and choose "Gradle" (assuming you have the Gradle plugin
installed). If you run into errors regarding class file versions, you may need to go to "File -> Project Structure..." 
to make sure that you have selected the correct JDK (21 or higher) and set an appropriate language level.

## Contributing

Contributions are welcome! Check the [GitHub issues](https://github.com/msfroh/lucene-university/issues) for requests
and suggestions for material to cover. If there is something else you think could use a worked example, feel free to
directly open a pull request with an example or create an issue requesting one.

Code examples should satisfy the following:

1. Each source file should be self-contained and should only import Lucene and Java classes. The example class should not inherit from 
anything else. If you need a small helper class, make it a `private static` inner class.
2. Each example class should have a `public static void main` method that clearly walks through the steps to demonstrate the given feature.
3. Each example should start with a comment with a large header (`// # This is title text`), and a summary explaining what the example
is about, before the `package` declaration.

## License

All code in this repository is licensed under the Apache License, Version 2.0. See the LICENSE file in the root of the repository for the
full text of the license.
