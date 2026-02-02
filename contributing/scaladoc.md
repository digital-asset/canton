ScalaDoc
========

Documentation on how to use ScalaDoc is quite sparse.
We recommend looking at these links for learning more about ScalaDoc usage.

* [Scala ScalaDoc Style Guide](https://docs.scala-lang.org/style/scaladoc.html)
* [ScalaDoc for Library Authors](https://docs.scala-lang.org/overviews/scaladoc/for-library-authors.html)
* [(a half complete but still one of the best) ScalaDoc Developer Guide](https://gist.githubusercontent.com/VladUreche/8396624/raw/fbff671119793ea3481f532b316e2612fae55fd9/gistfile1.txt)

To generate scaladoc run `sbt unidoc`

ScalaDoc generation is verified within our build pipeline so you may find your build failing due to incorrect ScalaDocs.
There a few common problem areas that you may hit.

# Unmoored ScalaDoc Comments

Remember that ScalaDoc can only be added to visible members.
`private` members are the obvious example of this however it can also apply elsewhere.

```scala
/** ScalaDoc is completely fine here */
def someMethod(): Unit = {
  /** This method however is not visible publicly.
   *  ScalaDoc will complain that this comment is unmoored.
   */
  def go(): Unit = {}
}
```

# Non ScalaDoc Comments

All comments beginning with `/**` will be considered a ScalaDoc comment.
If you don't actually want to create API documentation remember that `/*` works just fine.

# `@throws` type linking

The type provided to the `@throws` annotation must not use link syntax and be an absolute or a relative type reference.

**Incorrect:**

```
@throws UnsupportedOperationException whoopsie
@throws [[UnsupportedOperationException]] whoopsie
```


**Correct:**

```
@throws java.lang.UnsupportedOperationException whoopsie
```

# API Linking

Providing links to other members is one of the most frustrating aspects of ScalaDoc.
IntelliJ is frequently misleading in how it suggests you can link to other members (at least in a form that `sbt unidoc` supports).

There are a few things to keep in mind:
1. ScalaDoc does not process any `import` directives used in your scala source.
   ([There is a item to support this that has been open for nearly a decade](https://github.com/scala/bug/issues/3695), so any day now!?)

   All references must be absolute (full package aliased) or relative.
   Relative references can only reference a portion of the package reference if the location being documented and the reference both share a package ancestor.
2. Links can only be generated for members where API documentation exists.
   Notably api documentation for daml-lf libraries is not currently available, so although IntelliJ will happily create a Link for `GenTransaction` the build will fail as it cannot generate a url.
3. Disambiguating references between classes and a companion object can be done by using `!` and `$` prefixes.
4. Disambiguating references to overloaded methods can quickly get complicated.
   [See a variety of examples here for inspiration](https://github.com/scala/scala/blob/2.13.x/test/scaladoc/resources/links.scala#L39-L65).

   Absolute package references are typically required for types used when disambiguating methods.
   Even if a relative reference would otherwise work for a normal link.
