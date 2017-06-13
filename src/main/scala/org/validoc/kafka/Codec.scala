package org.validoc.kafka

trait Serializers[T]{
  def apply():String
}

object Serializers{
  implicit object SerializerForString extends Serializers[String] {
    override def apply(): String = "org.apache.kafka.common.serialization.StringSerializer"
  }
}
trait DeSerializers[T]{
  def apply():String
}

object DeSerializers{
  implicit object SerializerForString extends DeSerializers[String] {
    override def apply(): String = "org.apache.kafka.common.serialization.StringDeserializer"
  }
}