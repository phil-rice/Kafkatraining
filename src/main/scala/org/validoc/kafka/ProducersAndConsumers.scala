package org.validoc.kafka

import java.security.Key
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.validoc.kafka.JavaFutures._

import scala.collection.JavaConverters._
import scala.collection.parallel.ParIterable
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.{ExecutionContext, Future}

trait TopicProducer[K, V] {
  def topicName: String

  def send(key: K, value: V): Future[RecordMetadata]
}

class TopicProducerForKafka[K, V](defn: KafkaProducerDefn[K, V], val topicName: String)(implicit ex: ExecutionContext, jfc: JavaFutureContext, propertiesFor: PropertiesFor[KafkaProducerDefn[K, V]]) extends TopicProducer[K, V] {
  private val producer = new KafkaProducer[K, V](propertiesFor.propertiesFor(defn))

  override def send(key: K, value: V) = producer.send(new ProducerRecord[K, V]("test", key, value)).toScala
}


case class KeyValue[K, V](k: K, v: V)

trait RecordRipper[K, V, T] {
  def kv(t: T): KeyValue[K, V]

}

object RecordRipper {
  implicit def recordRipperForConsumerRecord[K, V] = new RecordRipper[K, V, ConsumerRecord[K, V]] {
    override def kv(r: ConsumerRecord[K, V]) = KeyValue(r.key(), r.value)
  }
}

trait TopicPoller[Record] {
  def poll(timeOut: Int): Iterable[Record]

  def close

}

class TopicConsumer[K, V, Record](topicPoller: TopicPoller[Record], timeOut: Int)(fn: ParIterable[KeyValue[K, V]] => Unit, errors: Throwable => Unit)(implicit recordRipper: RecordRipper[K, V, Record]) {
  def start = {
    while (true)
      try {
        fn(topicPoller.poll(timeOut).par.map(recordRipper.kv))
      } catch {
        case t: Throwable => errors(t)
      }
  }
}

object TopicConsumer {
  def apply[K, V](defn: KafkaConsumerDefn[K, V], topicName: String,  errors: Throwable => Unit, timeOut: Int)(fn: ParIterable[KeyValue[K, V]] => Unit)
                 (implicit recordRipper: RecordRipper[K, V, ConsumerRecord[K, V]],
                  keyDeserializer: DeSerializers[K],
                  valueDeserializer: DeSerializers[V]) = {
    val topicPoller = new TopicPollerKafka(defn, topicName)
    new TopicConsumer(topicPoller, timeOut)(fn, errors)
  }
}


class TopicPollerKafka[K, V](defn: KafkaConsumerDefn[K, V], val topicName: String)(implicit propertiesFor: PropertiesFor[KafkaConsumerDefn[K, V]]) extends TopicPoller[ConsumerRecord[K, V]] {
  private val consumer = new KafkaConsumer[K, V](propertiesFor.propertiesFor(defn))
  private val list = new util.ArrayList[String]()
  list.add(topicName)
  consumer.subscribe(list)

  override def poll(timeOut: Int) = iterableAsScalaIterableConverter(consumer.poll(timeOut)).asScala

  override def close: Unit = consumer.close
}

