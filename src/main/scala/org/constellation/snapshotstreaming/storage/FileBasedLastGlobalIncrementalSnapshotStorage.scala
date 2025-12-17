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
import io.constellationnetwork.node.shared.domain.snapshot.Validator.isNextSnapshot
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.tokenLock.TokenLockOrdinal
import io.constellationnetwork.security._
import io.constellationnetwork.validator.StateProofValidator


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

  def make[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer: Compression](
    path: Path, mptStore: MptStore[F, GlobalStateKey], globalStateProofSelector: GlobalStateProofSelector
  ): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] = {

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

    readSnapshotWithState.flatMap(Ref.of[F, Option[SnapshotWithState]](_).map(make(_, path, mptStore, globalStateProofSelector)))
  }

  def make[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer: Compression](
    cachedSnapshot: Ref[F, Option[SnapshotWithState]],
    path: Path, mptStore: MptStore[F, GlobalStateKey],
    globalStateProofSelector: GlobalStateProofSelector
  ): LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] = {
    new LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      implicit val stateProofSelector = globalStateProofSelector

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] = {
        JsonSerializer.forAsync.flatMap { implicit jsonSerializer =>
          HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
            state.stateProof(mptStore.underlying, snapshot.ordinal).flatMap { stateProof =>
              StateProofValidator.validate(snapshot, stateProof)
            }.flatMap(Async[F].fromValidated)
          }
        }
      }

      private def doSet(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] = {
        val snapshotWithState = SnapshotWithState(snapshot, state)
        Files[F].move(path, Path(path.toString + ".bk"), CopyFlags(ReplaceExisting, AtomicMove)).handleErrorWith {
          case _: java.nio.file.NoSuchFileException => Async[F].pure(None)
          case other => Async[F].raiseError(other)
        } >> saveSnapshotWithStateJson(path, snapshotWithState) >> cachedSnapshot.set(Some(snapshotWithState))
      }

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        validateStateProof(snapshot, state) >> {
          cachedSnapshot.get.flatMap {
            case Some(current) if isNextSnapshot(current.snapshot, snapshot.signed.value) =>
              doSet(snapshot, state)
            case Some(current) if current.snapshot.hash === snapshot.hash =>
              Async[F].unit // Same snapshot, idempotent
            case None =>
              setInitial(snapshot, state)
            case _ =>
              Async[F].raiseError(new Throwable("Failure during setting new global snapshot!"))
          }
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

}
