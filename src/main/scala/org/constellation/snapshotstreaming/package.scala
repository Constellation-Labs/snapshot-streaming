package org.constellation

import cats.MonadError
import cats.effect.Temporal
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

}
