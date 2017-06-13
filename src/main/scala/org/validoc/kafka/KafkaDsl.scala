package org.validoc.kafka

import java.util.Properties


trait PropertiesFor[T] {
  def propertiesFor(t: T): Properties
}


case class KafkaConsumerDefn[K,V](groupId: String,  hostAndPort: String = "localhost:9092", autoCommit: Boolean = true, commitIntervalMs: Int = 1000)

object KafkaConsumerDefn {

  implicit def PropertiesForConsumer[K,V](implicit keyDeserializer: DeSerializers[K],
                                          valueDeserializer: DeSerializers[V])=new PropertiesFor[KafkaConsumerDefn[K,V]] {
    override def propertiesFor(t: KafkaConsumerDefn[K,V]): Properties = {
      val props = new Properties()
      import t._
      props.put("bootstrap.servers", hostAndPort)
      props.put("group.id", groupId)
      props.put("enable.auto.commit", Boolean.box(autoCommit))
      props.put("auto.commit.interval.ms", new Integer(commitIntervalMs))
      props.put("key.deserializer", keyDeserializer())
      props.put("value.deserializer", valueDeserializer())
      props
    }
  }

}


case class KafkaProducerDefn[K,V](                                  hostAndPort: String = "localhost:9092",
                                  retries: Int = 0,
                                  batchSize:Int = 16384,
                                  lingerMs:Int = 1,
                                  bufferMemory:Int = 33554432 )

object KafkaProducerDefn {

  implicit def PropertiesForProducer[K,V](implicit keySerializer: Serializers[K], valueSerializer: Serializers[V]) = new PropertiesFor[KafkaProducerDefn[K,V]] {
    override def propertiesFor(t: KafkaProducerDefn[K,V]): Properties = {
      import t._
      val props = new Properties()
      props.put("bootstrap.servers", hostAndPort)
      props.put("acks", "all")
      props.put("retries", new Integer(retries))
      props.put("batch.size", new Integer(batchSize))
      props.put("linger.ms", new Integer(lingerMs))
      props.put("buffer.memory", new Integer(bufferMemory))
      props.put("key.serializer", keySerializer())
      props.put("value.serializer", valueSerializer())
      props
    }
  }
}