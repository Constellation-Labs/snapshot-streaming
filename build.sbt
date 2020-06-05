name := "cl-snapshot-streaming"

version := "0.1.0"

scalaVersion := "2.12.10"

lazy val versions = new {
  val catsCore = "2.0.0"
  val catsEffect = "2.1.3"
  val fs2 = "2.2.1"
  val constellation = "2.8.3"
  val scopt = "4.0.0-RC2"
  val logback = "1.2.3"
  val log4cats = "1.1.1"
  val scalaLogging = "3.9.2"
}

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % versions.catsCore,
  "org.typelevel" %% "cats-effect" % versions.catsEffect,
  "co.fs2" %% "fs2-core" % versions.fs2,
  "com.github.scopt" %% "scopt" % versions.scopt,
  "org.constellation" %% "cl-node" % versions.constellation from s"https://github.com/Constellation-Labs/constellation/releases/download/v${versions.constellation}/cl-node.jar" exclude("org.slf4j", "*"),
)

lazy val loggerDependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % versions.scalaLogging,
  "ch.qos.logback" % "logback-classic" % versions.logback,
  "io.chrisdavenport" %% "log4cats-slf4j" % versions.log4cats,
)

libraryDependencies ++= (dependencies ++ loggerDependencies)






