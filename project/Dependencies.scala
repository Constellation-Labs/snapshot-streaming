import sbt._

object Dependencies {

  object V {
    val amazonaws = "1.12.744"
    val cats = "2.9.0"
    val catsEffect = "3.4.2"
    val circe = "0.14.3"
    val config = "1.4.2"
    val derevo = "0.13.0"
    val elastic4s = "8.5.4"
    val fs2 = "3.4.0"
    val guava = "31.1-jre"
    val http4s = "0.23.16"
    val skunk = "1.1.0-M3"
    val pureconfig = "0.17.5"
    val log4cats = "2.5.0"
    val logback = "1.3.5"
    val logstash = "7.2"
    val organizeImports = "0.6.0"
    val refined = "0.10.1"
    val tessellation = "2.12.3-134-9003e0ee-dirty-SNAPSHOT"
    val weaver = "0.8.1"
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

    val awss3 = "com.amazonaws" % "aws-java-sdk-s3" % V.amazonaws

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

    val skunk = "org.tpolecat" %% "skunk-core" % V.skunk
    val skunkCirce = "org.tpolecat" %% "skunk-circe" % V.skunk
    val pureconfigCore = "com.github.pureconfig" %% "pureconfig" % V.pureconfig
    val pureconfigEnumeratum = "com.github.pureconfig" %% "pureconfig-enumeratum" % V.pureconfig

    val http4sCirce = http4s("circe")
    val http4sClient = http4s("ember-client")
    val http4sDsl = http4s("dsl")

    val log4cats = "org.typelevel" %% "log4cats-slf4j" % V.log4cats
    val logback = "ch.qos.logback" % "logback-classic" % V.logback
    val logstash = "net.logstash.logback" % "logstash-logback-encoder" % V.logstash

    val organizeImports = "com.github.liancheng" %% "organize-imports" % V.organizeImports

    val tessellationSdk = "io.constellationnetwork" %% "tessellation-sdk" % V.tessellation

    val weaverCats = weaver("cats")
    val weaverScalaCheck = weaver("scalacheck")

  }

}
