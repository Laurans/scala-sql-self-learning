# scala-sql-self-learning

Goal of this repo is to learn the scala language from scratch through an exercice: 
 - describing a postgres table without knowing the entire data model (we only know the table name and the posgres schema where it belongs).

At the end it produce some statistics on text and numeric columns. However, there is no output produced like a csv.

Postgres database is created with the other project `data-engineering-sandbox`.

## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).
