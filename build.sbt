import Dependencies._

ThisBuild / organization := "org.constellation"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "2.0.0"
ThisBuild / scalafixDependencies += Libraries.organizeImports
ThisBuild / assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)            => MergeStrategy.discard
  case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.last
  case x                                        => MergeStrategy.first
}

lazy val testSettings = Seq(
  testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
  libraryDependencies ++= Seq(
    Libraries.catsEffectTestKit,
    Libraries.weaverCats,
    Libraries.weaverScalaCheck
  ).map(_ % Test)
)

lazy val commonSettings = Seq(
  scalacOptions ++= List("-Ymacro-annotations", "-Yrangepos", "-Wconf:cat=unused:info", "-language:reflectiveCalls", "-Ywarn-unused"),
  resolvers ++= List(
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val core = (project in file("."))
  .settings(
    name := "cl-snapshot-streaming",
    commonSettings,
    testSettings,
    libraryDependencies ++= Seq(
      Libraries.catsCore,
      Libraries.catsKernel,
      Libraries.catsEffect,
      Libraries.circeCore,
      Libraries.circeGeneric,
      Libraries.derevoCirce,
      Libraries.derevoScalacheck,
      Libraries.config,
      Libraries.elasticCirce,
      Libraries.elasticClient,
      Libraries.elasticCore,
      Libraries.catsCore,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.guava,
      Libraries.http4sCirce,
      Libraries.http4sClient,
      Libraries.http4sDsl,
      Libraries.log4cats,
      Libraries.logback,
      Libraries.logstash,
      Libraries.tessellation
    )
  )
