package org.constellation.snapshotstreaming.serializer

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import org.constellation.schema.serialization.SchemaKryoRegistrar

class KryoSerializer extends Serializer {

  private val kryoPool = KryoPool.withByteArrayOutputStream(
    10,
    new ScalaKryoInstantiator()
      .setRegistrationRequired(true)
      .withRegistrar(SchemaKryoRegistrar)
  )

  def serialize(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserialize[T](message: Array[Byte]): T =
    kryoPool.fromBytes(message).asInstanceOf[T]
}
