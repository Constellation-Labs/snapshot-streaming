package org.constellation.snapshotstreaming

import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.security.hash.Hash

import derevo.circe.magnolia.decoder
import derevo.derive

@derive(decoder)
case class InitialSnapshot(hash: Hash, ordinal: SnapshotOrdinal)
