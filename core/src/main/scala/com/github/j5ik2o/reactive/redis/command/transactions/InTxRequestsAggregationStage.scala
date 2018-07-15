package com.github.j5ik2o.reactive.redis.command.transactions

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage._
import com.github.j5ik2o.reactive.redis.ResponseContext
import com.github.j5ik2o.reactive.redis.command.SimpleCommandRequest

import scala.collection.mutable.ListBuffer

object InTxRequestsAggregationFlow {
  def apply(): Flow[ResponseContext, ResponseContext, NotUsed] = Flow.fromGraph(InTxRequestsAggregationStage())
}

case class InTxRequestsAggregationStage() extends GraphStage[FlowShape[ResponseContext, ResponseContext]] {
  private val in  = Inlet[ResponseContext]("InTxRequestsAggregationFlow.int")
  private val out = Outlet[ResponseContext]("InTxRequestsAggregationFlow.out")

  override def shape: FlowShape[ResponseContext, ResponseContext] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inTx: Boolean                                  = false
      private val inTxRequests: ListBuffer[SimpleCommandRequest] = ListBuffer[SimpleCommandRequest]()

      override def preStart(): Unit = {
        inTx = false
        inTxRequests.clear()
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val responseContext = grab(in)
            responseContext.requestContext.commandRequest match {
              case _: MultiRequest =>
                inTx = true
                log.debug("start tx")
                push(out, responseContext)
              case _: ExecRequest if inTx =>
                log.debug("finish tx")
                inTx = false
                push(out, responseContext.withRequestsInTx(inTxRequests.result))
                inTxRequests.clear()
              case cmdReq: SimpleCommandRequest if inTx =>
                log.debug(s"in tx: $cmdReq")
                inTxRequests.append(cmdReq)
                push(out, responseContext)
              case cmdReq =>
                log.debug(s"single command request: $cmdReq")
                push(out, responseContext)
            }
          }
        }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

    }

}
