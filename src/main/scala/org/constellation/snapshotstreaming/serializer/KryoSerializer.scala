package org.constellation.snapshotstreaming.serializer

import cats.effect.Concurrent
import org.constellation.schema.serialization.{Kryo, SchemaKryoRegistrar}

object KryoSerializer {

  def init[F[_]: Concurrent]: F[Unit] = Kryo.init(SchemaKryoRegistrar)

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    Kryo.serializeAnyRef(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    Kryo.deserializeCast(bytes)
}
