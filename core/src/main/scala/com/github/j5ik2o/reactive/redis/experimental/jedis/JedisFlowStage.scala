package com.github.j5ik2o.reactive.redis.experimental.jedis

import java.util.UUID
import java.{ lang, util }

import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.connection.{ PingFailed, PingRequest, PingSucceeded, PingSuspended }
import com.github.j5ik2o.reactive.redis.command.keys.{ KeysFailed, KeysRequest, KeysSucceeded, KeysSuspended }
import com.github.j5ik2o.reactive.redis.command.strings.{ BitPosRequest, _ }
import com.github.j5ik2o.reactive.redis.command.transactions._
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

object JedisFlowStage {
  type CommandResponseF[A] = (Response[A]) => CommandResponse

  final case class ResponseF[A](response: Response[A], f: CommandResponseF[A]) {
    def apply(): CommandResponse = f(response)
  }

}
@SuppressWarnings(
  Array("org.wartremover.warts.Null",
        "org.wartremover.warts.Var",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.MutableDataStructures")
)
class JedisFlowStage(host: String, port: Int, connectionTimeout: Option[Duration], socketTimeout: Option[Duration])(
    implicit ec: ExecutionContext
) extends GraphStageWithMaterializedValue[FlowShape[CommandRequestBase, CommandResponse], Future[Jedis]] {
  private val in  = Inlet[CommandRequestBase]("JedisFlow.in")
  private val out = Outlet[CommandResponse]("JedisFlow.out")
  import JedisFlowStage._
  override def shape: FlowShape[CommandRequestBase, CommandResponse] = FlowShape(in, out)

  // scalastyle:off
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Jedis]) = {
    val promise = Promise[Jedis]()
    val logic = new GraphStageLogic(shape) with StageLogging {

      private case class RequestWithResult(request: CommandRequestBase, result: Try[CommandResponse])

      private val requests: mutable.Queue[CommandRequestBase]      = mutable.Queue.empty
      private var inFlight: Int                                    = _
      private var completionState: Option[Try[Unit]]               = _
      private var resultCallback: AsyncCallback[RequestWithResult] = _

      private var jedis: Jedis = _

      private val DC = () => ()

      private def run[A, B](
          req: CommandRequestBase
      )(
          cmd: => A
      )(onSuccess: A => CommandResponse)(onFailure: Throwable => CommandResponse)(closer: () => Unit = DC) = {
        Future(cmd)
          .map(result => onSuccess(result))
          .recover {
            case t: JedisConnectionException =>
              throw t
            case t: Throwable =>
              onFailure(t)
          }
          .onComplete { tried =>
            closer()
            tried.foreach { r =>
              push(out, r)
            }
            val requestWithResult =
              RequestWithResult(req, tried)
            resultCallback.invoke(requestWithResult)
          }
      }

      private var transaction: Option[Transaction] = None

      private val txState = ArrayBuffer[ResponseF[_]]()

      private def tryToExecute(): Unit = {
        log.debug(s"pendingRequests = $requests, isAvailable(out) = ${isAvailable(out)}")
        if (requests.nonEmpty && isAvailable(out)) {
          inFlight += 1
          val request = requests.dequeue()
          request match {
            case p: PingRequest =>
              ping(p)
            case m: MultiRequest =>
              multi(m)
            case e: ExecRequest =>
              exec(e)
            case d: DiscardRequest =>
              dicard(d)
            case a: AppendRequest =>
              append(a)
            case bc: BitCountRequest =>
              bitCount(bc)
            case bf: BitFieldRequest =>
              bitField(bf)
            case bo: BitOpRequest =>
              bitOp(bo)
            case bp: BitPosRequest =>
              bitPos(bp)
            case s: SetRequest =>
              set(s)
            case s: SetNxRequest =>
              setNx(s)
            case s: SetExRequest =>
              setEx(s)
            case p: PSetExRequest =>
              pSetEx(p)
            case g: GetRequest =>
              get(g)
            case k: KeysRequest =>
              keys(k)
          }
        }
      }

      private def bitPos(bp: BitPosRequest): Unit = {
        transaction match {
          case Some(tc) =>
            run(bp) {
              bp.startAndEnd match {
                case None =>
                  tc.bitpos(bp.key, bp.bit == 1)
                case Some(v) =>
                  val bpr = v match {
                    case BitPosRequest.StartAndEnd(s, None) =>
                      new BitPosParams(s)
                    case BitPosRequest.StartAndEnd(s, Some(e)) =>
                      new BitPosParams(s, e)
                  }
                  tc.bitpos(bp.key, bp.bit == 1, bpr)
              }
            } { result =>
              txState.append(
                ResponseF[java.lang.Long](result, { p =>
                  BitPosSucceeded(UUID.randomUUID(), bp.id, result.get)
                })
              )
              BitPosSuspended(UUID.randomUUID(), bp.id)
            } { t =>
              BitPosFailed(UUID.randomUUID(), bp.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(bp) {
              bp.startAndEnd match {
                case None =>
                  jedis.bitpos(bp.key, bp.bit == 1)
                case Some(v) =>
                  val bpr = v match {
                    case BitPosRequest.StartAndEnd(s, None) =>
                      new BitPosParams(s)
                    case BitPosRequest.StartAndEnd(s, Some(e)) =>
                      new BitPosParams(s, e)
                  }
                  jedis.bitpos(bp.key, bp.bit == 1, bpr)
              }
            } { result =>
              BitPosSucceeded(UUID.randomUUID(), bp.id, result)
            } { t =>
              BitPosFailed(UUID.randomUUID(), bp.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def bitOp(bo: BitOpRequest): Unit = {
        transaction match {
          case Some(tc) =>
            val op = BitOP.valueOf(bo.operand.entryName)
            run(bo)(tc.bitop(op, bo.outputKey, bo.inputKey1, bo.inputKey2)) { result =>
              txState.append(ResponseF[java.lang.Long](result, { p =>
                BitOpSucceeded(UUID.randomUUID(), bo.id, p.get)
              }))
              BitOpSuspended(UUID.randomUUID(), bo.id)
            } { t =>
              BitOpFailed(UUID.randomUUID(), bo.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            val op = BitOP.valueOf(bo.operand.entryName)
            run(bo)(jedis.bitop(op, bo.outputKey, bo.inputKey1, bo.inputKey2)) { result =>
              BitOpSucceeded(UUID.randomUUID(), bo.id, result)
            } { t =>
              BitOpFailed(UUID.randomUUID(), bo.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def bitField(bf: BitFieldRequest): Unit = {
        transaction match {
          case Some(tc) =>
            run(bf)(tc.bitfield(bf.key, bf.options.flatMap(_.asString.split(" ")): _*)) { result =>
              txState.append(ResponseF[util.List[lang.Long]](result, { r =>
                BitFieldSucceeded(UUID.randomUUID(), bf.id, r.get.asScala.map(_.toLong))
              }))
              BitFieldSuspended(UUID.randomUUID(), bf.id)
            } { t =>
              BitFieldFailed(UUID.randomUUID(), bf.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(bf)(jedis.bitfield(bf.key, bf.options.flatMap(_.asString.split(" ")): _*)) { result =>
              BitFieldSucceeded(UUID.randomUUID(), bf.id, result.asScala.map(_.toLong))
            } { t =>
              BitFieldFailed(UUID.randomUUID(), bf.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def keys(k: KeysRequest): Unit = {
        import k._
        transaction match {
          case Some(tc) =>
            run(k)(tc.keys(pattern)) { result =>
              txState.append(
                ResponseF[util.Set[String]](result, { r =>
                  KeysSucceeded(UUID.randomUUID(), id, r.get.asScala.toSeq)
                })
              )
              KeysSuspended(UUID.randomUUID(), id)
            } { t =>
              KeysFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(k)(jedis.keys(pattern)) { r =>
              KeysSucceeded(UUID.randomUUID(), id, r.asScala.toSeq)
            } { t =>
              KeysFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def pSetEx(p: PSetExRequest) = {
        import p._
        transaction match {
          case Some(tc) =>
            run(p)(tc.psetex(key, millis.toMillis, value)) { r =>
              txState.append(ResponseF[String](r, { r =>
                PSetExSucceeded(UUID.randomUUID(), id)
              }))
              PSetExSuspended(UUID.randomUUID(), id)
            } { t =>
              PSetExFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.psetex(key, millis.toMillis, value)) { _ =>
              PSetExSucceeded(UUID.randomUUID(), id)
            } { t =>
              PSetExFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def exec(e: ExecRequest): Unit = {
        run(e)(transaction.fold(throw new Exception)(_.exec())) { _ =>
          ExecSucceeded(UUID.randomUUID(), e.id, txState.map(_.apply).result)
        } { t =>
          ExecFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
        } { () =>
          transaction = None
          txState.clear()
        }
      }

      private def dicard(d: DiscardRequest): Unit = {
        run(d)(transaction.fold(throw new Exception)(_.discard())) { _ =>
          DiscardSucceeded(UUID.randomUUID(), d.id)
        } { t =>
          DiscardFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
        } { () =>
          transaction = None
          transaction = None
          txState.clear()
        }
      }

      private def append(a: AppendRequest): Unit = {
        import a._
        transaction match {
          case Some(v) =>
            run(a)(v.append(key, value)) { r =>
              txState.append(ResponseF[java.lang.Long](r, { p =>
                AppendSucceeded(UUID.randomUUID(), id, p.get)
              }))
              AppendSuspended(UUID.randomUUID(), id)
            } { t =>
              AppendFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(a)(jedis.append(key, value))(result => AppendSucceeded(UUID.randomUUID(), id, result))(
              t => AppendFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
        }
      }

      private def bitCount(b: BitCountRequest): Unit = {
        import b._
        transaction match {
          case Some(tc) =>
            run(b) {
              startAndEnd match {
                case None =>
                  tc.bitcount(key)
                case Some(v) =>
                  tc.bitcount(key, v.start, v.end)
              }
            } { r =>
              txState.append(ResponseF[java.lang.Long](r, { p =>
                BitCountSucceeded(UUID.randomUUID(), id, p.get)
              }))
              BitCountSuspended(UUID.randomUUID(), id)
            } { t =>
              BitCountFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(b) {
              startAndEnd match {
                case None =>
                  jedis.bitcount(key)
                case Some(v) =>
                  jedis.bitcount(key, v.start, v.end)
              }
            }(result => BitCountSucceeded(UUID.randomUUID(), id, result)) { t =>
              BitCountFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def set(s: SetRequest): Unit = {
        import s._
        transaction match {
          case Some(tc) =>
            run(s)(tc.set(key, value)) { r =>
              txState.append(ResponseF[String](r, { _ =>
                SetSuspended(UUID.randomUUID(), id)
              }))
              SetSuspended(UUID.randomUUID(), id)
            }(
              t => SetFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
          case None =>
            run(s)(jedis.set(key, value))(_ => SetSucceeded(UUID.randomUUID(), id))(
              t => SetFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
        }
      }

      private def setNx(s: SetNxRequest): Unit = {
        import s._
        transaction match {
          case Some(tc) =>
            run(s)(tc.setnx(key, value)) { result =>
              txState.append(ResponseF[java.lang.Long](result, { r =>
                SetNxSucceeded(UUID.randomUUID(), id, r.get == 1L)
              }))
              SetNxSuspended(UUID.randomUUID(), id)
            }(
              t => SetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
          case None =>
            run(s)(jedis.setnx(key, value))(result => SetNxSucceeded(UUID.randomUUID(), id, result == 1L))(
              t => SetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
        }
      }

      private def setEx(s: SetExRequest): Unit = {
        import s._
        transaction match {
          case Some(tc) =>
            run(s)(tc.setex(key, expires.toSeconds.toInt, value)) { r =>
              txState.append(ResponseF[String](r, { _ =>
                SetExSucceeded(UUID.randomUUID(), id)
              }))
              SetExSuspended(UUID.randomUUID(), id)
            }(t => SetExFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t))))()
          case None =>
            run(s)(jedis.setex(key, expires.toSeconds.toInt, value))(
              _ => SetExSucceeded(UUID.randomUUID(), id)
            )(t => SetExFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t))))()
        }
      }

      private def get(g: GetRequest): Unit = {
        import g._
        transaction match {
          case Some(tc) =>
            run(g)(tc.get(key)) { result =>
              txState.append(ResponseF[String](result, { r =>
                GetSucceeded(UUID.randomUUID(), id, Option(r.get))
              }))
              GetSuspended(UUID.randomUUID(), id)
            }(
              t => GetFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
          case None =>
            run(g)(jedis.get(key))(result => GetSucceeded(UUID.randomUUID(), id, Option(result)))(
              t => GetFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            )()
        }
      }

      private def multi(m: MultiRequest): Unit = {
        run(m)(jedis.multi()) { result =>
          transaction = Some(result)
          MultiSucceeded(UUID.randomUUID(), m.id)
        } { t =>
          MultiFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
        }()
      }

      private def ping(p: PingRequest): Unit = {
        import p._
        transaction match {
          case Some(v) =>
            run(p)(v.ping) { r =>
              txState.append(ResponseF[String](r, { r =>
                PingSucceeded(UUID.randomUUID(), id, r.get)
              }))
              PingSuspended(UUID.randomUUID(), id)
            } { t =>
              PingFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.ping) {
              PingSucceeded(UUID.randomUUID(), id, _)
            } { t =>
              PingFailed(UUID.randomUUID(), id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def checkForCompletion(): Unit =
        if (inFlight == 0 && requests.isEmpty && isClosed(in)) {
          completionState match {
            case Some(Success(_)) =>
              log.debug("checkForCompletion:completeStage")
              completeStage()
            case Some(Failure(ex)) =>
              log.debug(s"checkForCompletion:failStage($ex)")
              failStage(ex)
            case None =>
              log.debug(s"checkForCompletion:failStage(IllegalStateException)")
              failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      private def handleResult(requestWithResult: RequestWithResult): Unit = requestWithResult match {
        case RequestWithResult(_, Success(result)) =>
          log.debug("result = {}", result)
          inFlight -= 1
          tryToExecute()
          if (!hasBeenPulled(in)) tryPull(in)
          checkForCompletion()
        case RequestWithResult(_, Failure(ex)) =>
          log.error("Exception during put", ex)
          failStage(ex)
      }

      override def preStart(): Unit = {
        log.info("preStart: start")
        completionState = None
        inFlight = 0
        resultCallback = getAsyncCallback[RequestWithResult](handleResult)
        jedis = (connectionTimeout, socketTimeout) match {
          case (Some(ct), None) =>
            new Jedis(host, port, if (ct.isFinite()) ct.toSeconds.toInt else 0)
          case (Some(ct), Some(st)) =>
            new Jedis(host,
                      port,
                      if (ct.isFinite()) ct.toSeconds.toInt else 0,
                      if (st.isFinite()) st.toSeconds.toInt else 0)
          case _ =>
            new Jedis(host, port)
        }
        promise.success(jedis)
        pull(in)
        log.info("preStart: finished")
      }

      override def postStop(): Unit = {
        log.info("postStop: start")
        jedis.close()
        log.info("postStop: finished")
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = {
            log.debug("onUpstreamFinish")
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            completionState = Some(Failure(ex))
            checkForCompletion()
          }

          override def onPush(): Unit = {
            log.debug("onPush")
            requests.enqueue(grab(in))
            tryToExecute()
          }

        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            log.debug("onPull")
            tryToExecute()
            if (!hasBeenPulled(in)) tryPull(in)
          }
        }
      )

    }
    (logic, promise.future)
  }
}
