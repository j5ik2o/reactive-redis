package com.github.j5ik2o.reactive.redis.util

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.github.j5ik2o.reactive.redis.ResponseContext
import com.github.j5ik2o.reactive.redis.command.CommandRequest
import com.github.j5ik2o.reactive.redis.command.transactions.{ DiscardRequest, ExecRequest, MultiRequest }

import scala.collection.mutable.ListBuffer

object InTxRequestsAggregationFlow {

  def apply(): Flow[ResponseContext, ResponseContext, NotUsed] = Flow.fromGraph(InTxRequestsAggregationStage())

  def create(): Flow[ResponseContext, ResponseContext, NotUsed] = apply()

}

final case class InTxRequestsAggregationStage() extends GraphStage[FlowShape[ResponseContext, ResponseContext]] {
  private val in  = Inlet[ResponseContext]("InTxRequestsAggregationFlow.int")
  private val out = Outlet[ResponseContext]("InTxRequestsAggregationFlow.out")

  override def shape: FlowShape[ResponseContext, ResponseContext] = FlowShape(in, out)

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.Var"))
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inTx: Boolean                            = false
      private val inTxRequests: ListBuffer[CommandRequest] = ListBuffer[CommandRequest]()

      override def preStart(): Unit = {
        inTx = false
        inTxRequests.clear()
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val responseContext = grab(in)
            responseContext.commandRequest match {
              case mr: MultiRequest =>
                inTx = true
                log.debug(s"$mr")
                push(out, responseContext)
              case er: ExecRequest if inTx =>
                log.debug(s"$er")
                inTx = false
                push(out, responseContext.withRequestsInTx(inTxRequests.result))
                inTxRequests.clear()
              case dr: DiscardRequest if inTx =>
                log.debug(s"$dr")
                inTx = false
                push(out, responseContext)
                inTxRequests.clear()
              case cmdReq: CommandRequest if inTx =>
                log.debug(s"InTxRequest = $cmdReq")
                inTxRequests.append(cmdReq)
                push(out, responseContext)
              case cmdReq =>
                log.debug(s"SingleCommandRequest: $cmdReq")
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
