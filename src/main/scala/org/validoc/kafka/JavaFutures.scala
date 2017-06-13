package org.validoc.kafka

import scala.concurrent.{ExecutionContext, Promise, Future => SFuture}
import java.util.concurrent.{Future => JFuture}

import scala.util.Try

class FutureWrapper[T](val future: JFuture[T]) {
  val promise = Promise.apply[T]()

  def checkAndCompleteIfDone = {
    val result = future.isDone
    if (result)
      promise.complete(Try(future.get))
    result
  }
}

object JavaFutureContext {
  def apply(exceptionHandler: (Throwable => Unit) = JavaFutureContext.defaultErrorHandler)(implicit ex: ExecutionContext) = new JavaFutureContext(exceptionHandler)

  def defaultErrorHandler(throwable: Throwable) = {
    println(throwable)
    throwable.printStackTrace()
    throwable match {
      case e: Exception =>
      case t => throw t
    }
  }
}

class JavaFutureContext(exceptionHandler: (Throwable => Unit) = JavaFutureContext.defaultErrorHandler)(implicit ex: ExecutionContext) {
  private val lock = new Object
  private var set = Set[FutureWrapper[_]]()
  private var newSet = Set[FutureWrapper[_]]()
  val myThread = new Thread {
    setDaemon(true)

    override def run(): Unit = while (true) try {
      val setToBeChecked = lock.synchronized(set)
      val setToBeRemoved: Set[FutureWrapper[_]] = setToBeChecked.filter(_.checkAndCompleteIfDone)
      lock.synchronized {
        set = set -- setToBeRemoved ++ newSet
        newSet = Set()
      }
      if (lock.synchronized(set.size == 0)) Thread.sleep(10)
    } catch {
      case e: Throwable => exceptionHandler(e)
    }

  }.start

  def add[T](f: JFuture[T]): SFuture[T] = {
    val wrapper = new FutureWrapper[T](f)
    lock.synchronized(set = set + wrapper)
    wrapper.promise.future
  }

}

object JavaFutures {

  def toScala[T](j: JFuture[T])(implicit ec: JavaFutureContext): SFuture[T] = ec.add(j)

  implicit class JFuturePimper[T](j: JFuture[T])(implicit ec: JavaFutureContext) {
    def toScala = ec.add(j)
  }

}
