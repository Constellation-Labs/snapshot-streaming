package org.constellation.snapshotstreaming.mapper

import java.util.Date

import org.constellation.schema.snapshot.StoredSnapshot
import org.constellation.snapshotstreaming.schema.{
  CheckpointBlock,
  EdgeHashType,
  HashSignature,
  Height,
  Id,
  LastTransactionRef,
  ObservationEdge,
  SignatureBatch,
  SignedObservationEdge,
  Snapshot,
  Transaction,
  TransactionEdge,
  TransactionEdgeData,
  TransactionOriginal,
  TypedEdgeHash
}

import org.constellation.schema.transaction.{Transaction => OriginalTransaction}

class StoredSnapshotMapper {

  def mapOriginalTransaction(
    transaction: OriginalTransaction
  ): TransactionOriginal =
    TransactionOriginal(
      edge = TransactionEdge(
        {
          val oe = transaction.edge.observationEdge
          ObservationEdge(
            oe.parents.map(
              p =>
                TypedEdgeHash(
                  p.hashReference,
                  EdgeHashType.AddressHash,
                  p.baseHash
                )
            ),
            TypedEdgeHash(
              oe.data.hashReference,
              EdgeHashType.TransactionDataHash,
              oe.data.baseHash
            )
          )
        }, {
          val soe = transaction.edge.signedObservationEdge
          SignedObservationEdge(
            SignatureBatch(
              soe.signatureBatch.hash,
              soe.signatureBatch.signatures
                .map(hs => HashSignature(hs.signature, Id(hs.id.hex)))
            )
          )
        }, {
          val d = transaction.edge.data
          TransactionEdgeData(
            d.amount,
            LastTransactionRef(d.lastTxRef.prevHash, d.lastTxRef.ordinal),
            d.fee,
            d.salt
          )
        }
      ),
      lastTxRef = LastTransactionRef(
        transaction.lastTxRef.prevHash,
        transaction.lastTxRef.ordinal
      ),
      isDummy = transaction.isDummy,
      isTest = transaction.isTest
    )

  def mapTransaction(storedSnapshot: StoredSnapshot, timestamp: Date): Seq[Transaction] =
    storedSnapshot.checkpointCache.flatMap(
      b =>
        b.checkpointBlock.transactions.map(t => {
          Transaction(
            t.hash,
            t.amount,
            t.dst.hash,
            t.src.hash,
            t.fee.getOrElse(0),
            t.isDummy,
            LastTransactionRef(t.lastTxRef.prevHash, t.lastTxRef.ordinal),
            storedSnapshot.snapshot.hash,
            b.checkpointBlock.soeHash,
            mapOriginalTransaction(t),
            timestamp
          )
        })
    )

  def mapCheckpointBlock(storedSnapshot: StoredSnapshot, timestamp: Date): Seq[CheckpointBlock] =
    storedSnapshot.checkpointCache.map(checkpointCache => {
      CheckpointBlock(
        checkpointCache.checkpointBlock.soeHash,
        Height(checkpointCache.height.min, checkpointCache.height.max),
        checkpointCache.checkpointBlock.transactions.map(_.hash),
        Seq.empty[String],
        Seq.empty[String],
        checkpointCache.children.toLong,
        storedSnapshot.snapshot.hash,
        checkpointCache.checkpointBlock.soeHash,
        checkpointCache.checkpointBlock.parentSOEHashes,
        timestamp
      )
    })

  def mapSnapshot(storedSnapshot: StoredSnapshot, timestamp: Date): Snapshot =
    Snapshot(
      storedSnapshot.snapshot.hash,
      storedSnapshot.height,
      storedSnapshot.snapshot.checkpointBlocks,
      timestamp
    )
}
