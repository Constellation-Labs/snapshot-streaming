name := "cl-snapshot-streaming"

version := "1.1.6"
scalaVersion := "2.12.10"
organization := "org.constellation"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)            => MergeStrategy.discard
  case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.last
  case x                                        => MergeStrategy.first
}

assemblyJarName in assembly := s"snapshot-streaming-assembly-${version.value}.jar"

lazy val versions = new {
  val catsCore = "2.0.0"
  val catsEffect = "2.1.3"
  val fs2 = "2.2.1"
  val fs2http = "0.4.0"
  val constellation = "2.24.6"
  val scopt = "4.0.0-RC2"
  val logback = "1.2.3"
  val log4cats = "1.1.1"
  val scalaLogging = "3.9.2"
  val http4s = "0.21.2"
  val elastic4sVersion = "7.3.1"
}

lazy val elastic4sDependencies = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core",
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava",
  "com.sksamuel.elastic4s" %% "elastic4s-http-streams"
).map(_ % versions.elastic4sVersion)

lazy val http4sDependencies = Seq(
  "org.http4s" %% "http4s-blaze-client",
  "org.http4s" %% "http4s-circe",
  "org.http4s" %% "http4s-dsl",
  "org.http4s" %% "http4s-okhttp-client"
).map(_ % versions.http4s)

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % versions.catsCore,
  "org.typelevel" %% "cats-effect" % versions.catsEffect,
  "co.fs2" %% "fs2-core" % versions.fs2,
  "com.spinoco" %% "fs2-http" % versions.fs2http,
  "com.github.scopt" %% "scopt" % versions.scopt,
  ("org.constellation" %% "cl-schema" % versions.constellation).from(
    s"https://github.com/Constellation-Labs/constellation/releases/download/v${versions.constellation}/cl-schema.jar"
  ),
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % "6.7.8",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.863"
)

lazy val loggerDependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % versions.scalaLogging,
  "ch.qos.logback" % "logback-classic" % versions.logback,
  "io.chrisdavenport" %% "log4cats-slf4j" % versions.log4cats
)

libraryDependencies ++= (dependencies ++ loggerDependencies ++ http4sDependencies ++ elastic4sDependencies)
