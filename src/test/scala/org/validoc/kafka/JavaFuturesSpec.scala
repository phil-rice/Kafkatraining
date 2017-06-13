package org.validoc.kafka

import java.util.concurrent.{Callable, CountDownLatch, Executors}

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
class JavaFuturesSpec extends FlatSpec with Matchers {


  behavior of "JavaFutures"

  it should "Allow a JAva Future to be converted to a SCala future" in {
    val executors = Executors.newFixedThreadPool(2)
    val jfc = new JavaFutureContext()
    val latch = new CountDownLatch(1)
    val jf = executors.submit(new Callable[Int] {
      override def call(): Int = {
        latch.await()
        1
      }
    })
    val sf = jfc.add(jf)
    val sf2 = sf.map(_ * 2)

    sf.isCompleted shouldBe false
    sf2.isCompleted shouldBe false

    latch.countDown()
    jf.get shouldBe 1

    Await.result(sf, 5 seconds ) shouldBe 1
    Await.result(sf2, 5 seconds ) shouldBe 2

  }
}
