import Dependencies._

enablePlugins(GitVersioning)

ThisBuild / organization := "org.constellation"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / scalafixDependencies += Libraries.organizeImports
ThisBuild / evictionErrorLevel := Level.Warn

githubTokenSource := (TokenSource.GitConfig("github.token") || TokenSource.Environment("GITHUB_TOKEN"))
git.useGitDescribe := true

ThisBuild / assemblyMergeStrategy := {
  case "logback.xml"                                       => MergeStrategy.first
  case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
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
  scalacOptions ++= List(
    "-Ymacro-annotations",
    "-Yrangepos",
    "-Wconf:cat=unused:info",
    "-language:reflectiveCalls",
    "-Ywarn-unused"
  ),
  resolvers ++= List(
    Resolver.sonatypeRepo("snapshots"),
    Resolver.githubPackages("abankowski", "http-request-signer"),
    Resolver.mavenLocal
  )
)

lazy val core = (project in file("."))
  .settings(
    name := "cl-snapshot-streaming",
    commonSettings,
    testSettings,
    libraryDependencies ++= Seq(
      Libraries.awss3,
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
      Libraries.tessellationSdk,
      Libraries.tessellationShared
    )
  )
