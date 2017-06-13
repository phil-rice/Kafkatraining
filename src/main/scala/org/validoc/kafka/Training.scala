package org.validoc.kafka

object Training extends App {

  import scala.concurrent.ExecutionContext.Implicits._

  implicit val jfc = new JavaFutureContext
  val producerDefn = KafkaProducerDefn[String, String]()
  val consumerDefn = KafkaConsumerDefn[String, String]("testGroup")

  val producer = new TopicProducerForKafka(producerDefn, "test")
  (1 to 100) map (i => producer.send(Integer.toString(i), Integer.toString(i)))
  TopicConsumer[String, String](consumerDefn, "test", _.printStackTrace(), 10) { parSeq => parSeq.foreach(println) }.start
}
