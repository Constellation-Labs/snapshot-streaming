package org.constellation.snapshotstreaming.storage

import java.nio.file.NoSuchFileException

import cats.effect.Async
import cats.effect.std.Mutex
import cats.syntax.all._
import cats.{Applicative, MonadThrow}

import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.domain.snapshot.Validator.isNextSnapshot
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.security._

import eu.timepit.refined.types.all.NonNegLong
import fs2.io.file._
import fs2.{Stream, text}
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax._
import io.circe.{Codec, jawn}

trait DirectoryBasedLastNGlobalIncrementalSnapshotStorage[F[_]]
    extends LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

  def getLastN(n: Long): F[Option[List[Hashed[GlobalIncrementalSnapshot]]]]

  def getLastNWithState(n: Long): F[Option[List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]]

  def getAll: F[Option[List[Hashed[GlobalIncrementalSnapshot]]]]
}

object DirectoryBasedLastNGlobalIncrementalSnapshotStorage {

  private case class SnapshotWithState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)

  def make[F[_]: Async: HasherSelector](
    dirPath: Path,
    maxSnapshots: Long
  ): F[DirectoryBasedLastNGlobalIncrementalSnapshotStorage[F]] =
    Mutex[F].map(make(_, dirPath, maxSnapshots))

  def make[F[_]: Async: HasherSelector](
    mutex: Mutex[F],
    dirPath: Path,
    maxSnapshots: Long
  ): DirectoryBasedLastNGlobalIncrementalSnapshotStorage[F] =
    new DirectoryBasedLastNGlobalIncrementalSnapshotStorage[F] {

      private implicit val codec: Codec[Hashed[GlobalIncrementalSnapshot]] =
        deriveCodec[Hashed[GlobalIncrementalSnapshot]]

      private implicit val snapshotWithInfoCodec: Codec[SnapshotWithState] = deriveCodec[SnapshotWithState]

      private def getSnapshotPath(ordinal: SnapshotOrdinal): Path =
        dirPath / s"snapshot_${ordinal.value.value}.json"

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
          (hasher.getLogic(snapshot.ordinal) match {
            case JsonHash => StateProofValidator.validate(snapshot, state)
            case KryoHash => StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(state))
          }).flatMap(Async[F].fromValidated)
        }

      private def readSnapshotFile(path: Path): F[Option[SnapshotWithState]] =
        Files[F]
          .readAll(path)
          .through(text.utf8.decode)
          .compile
          .toList
          .map(_.mkString)
          .map(jawn.decode[SnapshotWithState])
          .flatMap(_.liftTo[F])
          .map(_.some)
          .handleErrorWith {
            case _: NoSuchFileException => Applicative[F].pure(None)
            case e                      => e.raiseError[F, Option[SnapshotWithState]]
          }

      private def listSnapshotFiles: F[List[Path]] =
        Files[F]
          .list(dirPath)
          .filter(path => path.fileName.toString.startsWith("snapshot_") && path.fileName.toString.endsWith(".json"))
          .compile
          .toList

      private def pruneOldSnapshots(currentOrdinal: SnapshotOrdinal): F[Unit] = {
        val minOrdinalToKeep = Math.max(0, currentOrdinal.value.value - maxSnapshots + 1)

        listSnapshotFiles.flatMap { files =>
          files.traverse_ { path =>
            val fileName = path.fileName.toString
            val ordinalStr = fileName.stripPrefix("snapshot_").stripSuffix(".json")

            ordinalStr.toLongOption.flatMap(NonNegLong.from(_).toOption) match {
              case Some(ord) if ord.value < minOrdinalToKeep =>
                Files[F].delete(path)
              case _ =>
                Applicative[F].unit
            }
          }
        }
      }

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        mutex.lock.surround {
          validateStateProof(snapshot, state) >>
            get.flatMap {
              case Some(last) if isNextSnapshot(last, snapshot.signed.value) =>
                Stream
                  .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
                  .through(text.utf8.encode)
                  .through(Files[F].writeAll(getSnapshotPath(snapshot.ordinal)))
                  .compile
                  .drain >>
                  pruneOldSnapshots(snapshot.ordinal)
              case Some(last) =>
                MonadThrow[F].raiseError[Unit](
                  new IllegalStateException(
                    s"Snapshot is not the next one! last: ${SnapshotReference
                        .fromHashedSnapshot(last)}, lastHash: ${last.hash}, next: ${SnapshotReference
                        .fromHashedSnapshot(snapshot)}, prevHash: ${snapshot.signed.value.lastSnapshotHash}"
                  )
                )
              case None =>
                MonadThrow[F].raiseError[Unit](
                  new IllegalStateException("Previous snapshot not found when setting next global snapshot!")
                )
            }
        }

      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        mutex.lock.surround {
          validateStateProof(snapshot, state) >>
            Stream
              .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
              .through(text.utf8.encode)
              .through(Files[F].writeAll(getSnapshotPath(snapshot.ordinal), Flags(Flag.Write, Flag.CreateNew)))
              .compile
              .drain
        }

      def get: F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        getLatestSnapshot.map(_.map(_.snapshot))

      def getAll: F[Option[List[Hashed[GlobalIncrementalSnapshot]]]] =
        getAllSnapshots.map { snapshots =>
          if (snapshots.isEmpty) None
          else Some(snapshots.map(_.snapshot))
        }

      def getLatestSnapshot: F[Option[SnapshotWithState]] =
        listSnapshotFiles.flatMap { files =>
          if (files.isEmpty) {
            Applicative[F].pure(None)
          } else {
            // Extract ordinals from filenames and find the max
            val sortedFiles = files.flatMap { path =>
              val fileName = path.fileName.toString
              val ordinalStr = fileName.stripPrefix("snapshot_").stripSuffix(".json")

              ordinalStr.toLongOption
                .flatMap(NonNegLong.from(_).toOption)
                .map(ord => (path, ord.value))
            }.sortBy(_._2)(Ordering[Long].reverse)

            sortedFiles.headOption match {
              case Some((latestPath, _)) => readSnapshotFile(latestPath)
              case None                  => Applicative[F].pure(None)
            }
          }
        }

      def getAllSnapshots: F[List[SnapshotWithState]] =
        listSnapshotFiles.flatMap { files =>
          val sortedFiles = files.flatMap { path =>
            val fileName = path.fileName.toString
            val ordinalStr = fileName.stripPrefix("snapshot_").stripSuffix(".json")

            ordinalStr.toLongOption
              .flatMap(NonNegLong.from(_).toOption)
              .map(ord => (path, ord.value))
          }.sortBy(_._2)(Ordering[Long].reverse)

          sortedFiles.traverse { case (path, _) =>
            readSnapshotFile(path).map(_.toList)
          }.map(_.flatten)
        }

      def getCombined: F[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        getLatestSnapshot.map(_.map(sws => (sws.snapshot, sws.state)))

      def getCombinedStream: fs2.Stream[F, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        Stream.eval(getCombined)

      def getOrdinal: F[Option[SnapshotOrdinal]] = get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] = get.map(_.map(_.height))

      def getLastN(n: Long): F[Option[List[Hashed[GlobalIncrementalSnapshot]]]] =
        getAllSnapshots.map { snapshots =>
          val limitedSnapshots = snapshots.take(n.toInt).map(_.snapshot)
          if (limitedSnapshots.isEmpty) None else Some(limitedSnapshots)
        }

      def getLastNWithState(n: Long): F[Option[List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]] =
        getAllSnapshots.map { snapshots =>
          val limitedSnapshots = snapshots.take(n.toInt).map(sws => (sws.snapshot, sws.state))
          if (limitedSnapshots.isEmpty) None else Some(limitedSnapshots)
        }

    }

}
