package org.constellation

import cats.{FlatMap, MonadError}
import cats.effect.{Clock, Temporal}
import cats.effect.implicits.clockOps
import cats.syntax.all._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

package object snapshotstreaming {

  def retryF[F[_]: Logger, A](effect: F[A], maxRetries: Int = 5)(implicit
    F: MonadError[F, Throwable],
    timer: Temporal[F]
  ): F[A] =
    effect.handleErrorWith { case err: Throwable =>
      if (maxRetries > 0)
        Logger[F].warn(s"Error $err, retrying... (${maxRetries})") *> retryF(effect, maxRetries - 1)
      else
        F.raiseError(err)
    }

  def timed[A, F[_]: Logger: Clock: FlatMap](f: F[A])(msg: String): F[(FiniteDuration, A)] = f.timed.flatTap {
    case (t, _) => Logger[F].info(s"$msg in ${t.toSeconds} s.")
  }

}
