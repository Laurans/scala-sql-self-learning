val scala3Version = "3.3.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "hello-world",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.postgresql" % "postgresql" % "42.2.5", //org.postgresql.ds.PGSimpleDataSource dependency
      "org.scalikejdbc" %% "scalikejdbc"       % "4.0.0",
      "org.scalikejdbc" %% "scalikejdbc-config"  % "4.0.0",
      "ch.qos.logback"  %  "logback-classic"   % "1.2.3"
    )
  )
