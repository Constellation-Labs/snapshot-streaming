package org.constellation.snapshotstreaming.storage

import cats.{Applicative, Parallel}
import cats.effect._
import cats.syntax.all._
import fs2.compression.Compression
import fs2.io.file.CopyFlag.{AtomicMove, ReplaceExisting}
import fs2.io.file._
import fs2.{Stream, text}
import io.circe.jawn
import io.circe.syntax._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax.GlobalSnapshotInfoMptOps
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.tokenLock.TokenLockOrdinal
import io.constellationnetwork.security._
import org.typelevel.log4cats.slf4j.Slf4jLogger


object FileBasedLastGlobalIncrementalSnapshotStorage {

  def saveSnapshotWithStateJson[F[_]: Files: Compression: Async]( filePath: Path,
                                 snapshotWithState: SnapshotWithState,
                                 flags: Flags = Flags(Flag.Create,Flag.Write, Flag.Truncate)
                               ): F[Unit] =
    Stream
      .emit(snapshotWithState.asJson.spaces2)
      .through(text.utf8.encode)
      .through(Compression[F].gzip())
      .through(Files[F].writeAll(filePath, flags))
      .compile
      .drain

  def make[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer: Compression: JsonSerializer](
    path: Path,
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: StateProofSelector): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] = {

    def deserializeWithJson(data: Array[Byte]) = jawn.decode[SnapshotWithState](new String(data, "UTF-8"))

    val readSnapshotWithState: F[Option[SnapshotWithState]] =
      Files[F]
        .readAll(path)
        .through(Compression[F].gunzip())
        .flatMap(_.content)
        .compile
        .to(Array)
        .map(deserializeWithJson)
        .flatMap(_.liftTo[F])
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[SnapshotWithState]]
        }

    readSnapshotWithState.flatMap(Ref.of[F, Option[SnapshotWithState]](_).map(make(_, path, mptStore)))
  }

  def make[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer: Compression: JsonSerializer](
    cachedSnapshot: Ref[F, Option[SnapshotWithState]],
    path: Path,
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: StateProofSelector): LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
          (hasher.getLogic(snapshot.ordinal) match {
            case JsonHash => StateProofValidator.validate(snapshot, state, mptStore)
            case KryoHash =>
              StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(state), mptStore)
          }).flatMap(Async[F].fromValidated)
        }

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] = {
        for {
//          kvPairs <- HasherSelector[F].withCurrent(implicit hasher => state.allStateEntries[F])
//          _ <- mptStore.sync(kvPairs, snapshot.ordinal)
//          _ <- logger.info("Validating stateProof before save file")
//          _ <- validateStateProof(snapshot, state)
          _ <- cachedSnapshot.get.flatMap { x =>
            x.map { _ =>
              val snapshotWithState = SnapshotWithState(snapshot, state)
              //move previous bk file
              Files[F].move(path,Path(path.toString + ".bk"),  CopyFlags(ReplaceExisting, AtomicMove)).handleErrorWith {
                case _: java.nio.file.NoSuchFileException => Async[F].pure(None)
                case other => Async[F].raiseError(other) // Re-raise other errors
              } >>
                saveSnapshotWithStateJson(path, snapshotWithState) >> cachedSnapshot.set(Some(snapshotWithState))
            }.getOrElse(setInitial(snapshot, state))
          }
        } yield ()
      }


      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] = {
        val snapshotWithState = SnapshotWithState(snapshot, state)
        validateStateProof(snapshot, state) >> saveSnapshotWithStateJson(path,
          snapshotWithState,
          Flags(Flag.Write, Flag.CreateNew)
        ) >> cachedSnapshot.set(Some(snapshotWithState))
      }

      def get: F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        cachedSnapshot.get.map(_.map(_.snapshot))

      def getCombined: F[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        cachedSnapshot.get.map(_.map(sws => (sws.snapshot, sws.state)))

      def getCombinedStream: fs2.Stream[F, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        Stream.eval(getCombined)

      def getOrdinal: F[Option[SnapshotOrdinal]] = get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] = get.map(_.map(_.height))

    }

}
