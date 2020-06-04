import cats.effect.{Concurrent}
import cats.implicits._
import scopt.OParser

object Config {

  case class CliConfig(bucket: String = null, region: String = "us-west-1")

  def loadCliParams[F[_]: Concurrent](
    args: List[String]
  ): F[CliConfig] = {
    val builder = OParser.builder[CliConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("x"),
        opt[String]("bucket")
          .action((x, c) => c.copy(bucket = x))
          .text("bucket to stream from")
          .required(),
        opt[String]("region")
          .action((x, c) => c.copy(region = x))
          .text("region of bucket"),
      )
    }

      OParser
        .parse(parser, args, CliConfig())
        .toRight(new RuntimeException("CLI params are missing"))
        .toEitherT[F]
        .rethrowT
    }

}
