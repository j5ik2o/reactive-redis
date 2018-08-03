package com.github.j5ik2o.reactive.redis.util

import java.util.concurrent.Semaphore

import akka.actor.ActorRef
import akka.stream._
import akka.stream.stage._

import scala.concurrent.{ Future, Promise }

@SuppressWarnings(
  Array("org.wartremover.warts.Null",
        "org.wartremover.warts.Var",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.MutableDataStructures")
)
class ActorSourceStage[E](bufferSize: Int) extends GraphStageWithMaterializedValue[SourceShape[E], Future[ActorRef]] {
  private val out = Outlet[E]("ActorSource.out")

  override def shape: SourceShape[E] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[ActorRef]) = {
    val promise = Promise[ActorRef]()
    val logic = new GraphStageLogic(shape) with StageLogging {
      private var semaphore: Semaphore = _

      private val queue = new java.util.ArrayDeque[E]()

      private def tryToExecute(): Unit = {
        if (!queue.isEmpty && isAvailable(out)) {
          val element = queue.poll()
          log.debug("element = {}", element)
          push(out, element)
          semaphore.release()
        }
      }

      override def preStart(): Unit = {
        semaphore = new Semaphore(bufferSize)
        val sa = getStageActor {
          case (_, msg) =>
            log.debug("message = {}", msg)
            semaphore.acquire()
            queue.add(msg.asInstanceOf[E])
            tryToExecute()
        }
        promise.success(sa.ref)
      }

      override def postStop(): Unit = {
        queue.clear()
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          tryToExecute()
        }
      })

    }
    (logic, promise.future)
  }

}
