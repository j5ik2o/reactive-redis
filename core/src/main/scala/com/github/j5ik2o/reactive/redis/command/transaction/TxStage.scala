package com.github.j5ik2o.reactive.redis.command.transaction

import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage._
import com.github.j5ik2o.reactive.redis.ResponseContext
import com.github.j5ik2o.reactive.redis.command.SimpleCommandRequest

import scala.collection.mutable.ListBuffer

class TxStage extends GraphStage[FlowShape[ResponseContext, ResponseContext]] {
  private val in  = Inlet[ResponseContext]("TxStage.int")
  private val out = Outlet[ResponseContext]("TxStage.out")

  override def shape: FlowShape[ResponseContext, ResponseContext] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      private var inTx: Boolean = false
      private val inTxReqs      = ListBuffer[SimpleCommandRequest]()

      override def preStart(): Unit = {
        inTx = false
        inTxReqs.clear()
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
                if (isAvailable(out))
                  push(out, responseContext)
              case _: ExecRequest if inTx =>
                log.debug("finish tx")
                inTx = false
                if (isAvailable(out))
                  push(out, responseContext.withRequestsInTx(inTxReqs.result))
                inTxReqs.clear()
              case cmdReq: SimpleCommandRequest if inTx =>
                log.debug(s"in tx: $cmdReq")
                inTxReqs.append(cmdReq)
                if (isAvailable(out))
                  push(out, responseContext)
              case cmdReq =>
                log.debug(s"no tx: $cmdReq")
                if (isAvailable(out))
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
