package org.constellation

import cats.{FlatMap, MonadError}
import cats.effect.{Clock, Temporal}
import cats.effect.implicits.clockOps
import cats.syntax.all._
import org.typelevel.log4cats.Logger

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

  implicit class TimedLogOps[F[_], A](private val fa: F[A]) extends AnyVal {
    def timedLog(msg: String)(implicit L: Logger[F], C: Clock[F], F: FlatMap[F]): F[A] =
      fa.timed.flatTap { case (t, _) => L.info(s"$msg in ${t.toSeconds} s.") }.map(_._2)
  }
}
