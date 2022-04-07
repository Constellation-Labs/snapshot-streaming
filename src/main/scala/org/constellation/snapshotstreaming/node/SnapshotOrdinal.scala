package org.constellation.snapshotstreaming.node

import cats.kernel.Order
import cats.syntax.contravariant._

case class SnapshotOrdinal(value: Long)

object SnapshotOrdinal {

  implicit class LongToSnapshot(snapshotsOrdinalValue: Long) {
    @inline def toSnapshotOrdinal: SnapshotOrdinal = SnapshotOrdinal(snapshotsOrdinalValue)
  }

  implicit class SnapshotOrdinalOps(snapshotOrdinal: SnapshotOrdinal) {
    @inline def increment: SnapshotOrdinal = (snapshotOrdinal.value + 1).toSnapshotOrdinal
  }

  implicit val order: Order[SnapshotOrdinal] = Order[Long].contramap(_.value)
  implicit val ordering: Ordering[SnapshotOrdinal] = order.toOrdering
  implicit def snapshotOrdinalToOrdinalOps(so: SnapshotOrdinal): Ordering[SnapshotOrdinal]#OrderingOps = so
}
