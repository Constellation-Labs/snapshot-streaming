import sbt._

object Dependencies {

  object V {
    val cats = "2.7.0"
    val catsEffect = "3.3.11"
    val circe = "0.14.0"
    val config = "1.4.2"
    val derevo = "0.12.6"
    val elastic4s = "8.1.0"
    val fs2 = "3.2.7"
    val guava = "30.1.1-jre"
    val http4s = "0.23.11"
    val log4cats = "2.2.0"
    val logback = "1.2.11"
    val logstash = "7.0.1"
    val organizeImports = "0.6.0"
    val refined = "0.9.28"
    val tessellation = "0.11.0"
    val weaver = "0.7.11"
  }

  object Libraries {
    def cats(artifact: String): ModuleID = "org.typelevel" %% s"cats-$artifact" % V.cats

    def catsEffect(artifact: String): ModuleID =
      "org.typelevel" %% s"cats-effect${if (artifact.nonEmpty) "-" + artifact else ""}" % V.catsEffect

    def circe(artifact: String): ModuleID = "io.circe" %% s"circe-$artifact" % V.circe

    def derevo(artifact: String): ModuleID = "tf.tofu" %% s"derevo-$artifact" % V.derevo

    def elastic(artifact: String): ModuleID =
      ("com.sksamuel.elastic4s" %% s"elastic4s-$artifact" % V.elastic4s).cross(CrossVersion.for3Use2_13)

    def fs2(artifact: String): ModuleID = "co.fs2" %% s"fs2-$artifact" % V.fs2

    def http4s(artifact: String): ModuleID = "org.http4s" %% s"http4s-$artifact" % V.http4s

    def weaver(artifact: String): ModuleID = "com.disneystreaming" %% s"weaver-$artifact" % V.weaver

    val catsCore = cats("core")
    val catsKernel = cats("kernel")

    val catsEffect: ModuleID = catsEffect("")
    val catsEffectTestKit = catsEffect("testkit")

    val circeCore = circe("core")
    val circeGeneric = circe("generic")

    val config = "com.typesafe" % "config" % V.config

    val derevoCirce = derevo("circe-magnolia")
    val derevoScalacheck = derevo("scalacheck")

    val elasticCirce = elastic("json-circe")
    val elasticClient = elastic("client-esjava")
    val elasticCore = elastic("core")

    val fs2Core = fs2("core")
    val fs2Io = fs2("io")

    val guava = "com.google.guava" % "guava" % V.guava

    val http4sCirce = http4s("circe")
    val http4sClient = http4s("ember-client")
    val http4sDsl = http4s("dsl")

    val log4cats = "org.typelevel" %% "log4cats-slf4j" % V.log4cats
    val logback = "ch.qos.logback" % "logback-classic" % V.logback
    val logstash = "net.logstash.logback" % "logstash-logback-encoder" % V.logstash

    val organizeImports = "com.github.liancheng" %% "organize-imports" % V.organizeImports

    val tessellation = ("org.constellation" %% "tessellation-core" % V.tessellation).from(
      s"https://github.com/Constellation-Labs/tessellation/releases/download/v${V.tessellation}/cl-node.jar"
    )

    val weaverCats = weaver("cats")
    val weaverScalaCheck = weaver("scalacheck")

  }

}
