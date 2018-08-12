package com.github.j5ik2o.reactive.redis.experimental.jedis

import java.util.UUID
import java.{ lang, util }

import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import cats.Id
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.connection._
import com.github.j5ik2o.reactive.redis.command.hashes._
import com.github.j5ik2o.reactive.redis.command.keys._
import com.github.j5ik2o.reactive.redis.command.lists._
import com.github.j5ik2o.reactive.redis.command.sets.{ SAddFailed, SAddRequest, SAddSucceeded, SAddSuspended }
import com.github.j5ik2o.reactive.redis.command.strings.{ BitPosRequest, _ }
import com.github.j5ik2o.reactive.redis.command.transactions._
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import redis.clients.jedis.Protocol.Command
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
  trait JedisEx[M[_]] {
    def pingArg(arg: Option[String]): M[String]
  }

}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.ToString",
    "org.wartremover.warts.MutableDataStructures"
  )
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

      private var jedis: Jedis with JedisEx[Id] = _

      private val DC = () => ()

      private def run[A, B](
          req: CommandRequestBase
      )(
          cmd: => A
      )(
          onSuccess: A => CommandResponse
      )(onFailure: Throwable => CommandResponse)(closer: () => Unit = DC): Future[CommandResponse] = {
        val f = Future(cmd)
          .map(result => onSuccess(result))
          .recover {
            case t: JedisConnectionException =>
              throw t
            case t: Throwable =>
              onFailure(t)
          }

        f.onComplete { tried =>
          closer()
          tried.foreach { r =>
            push(out, r)
          }
          val requestWithResult =
            RequestWithResult(req, tried)
          resultCallback.invoke(requestWithResult)
        }
        f
      }

      private var transaction: Option[Transaction] = None

      private val txState = ArrayBuffer[ResponseF[_]]()

      private def tryToExecute(): Unit = {
        log.debug(s"pendingRequests = $requests, isAvailable(out) = ${isAvailable(out)}")
        if (requests.nonEmpty && isAvailable(out)) {
          inFlight += 1
          val request = requests.dequeue()
          request match {
            // connections
            case q: QuitRequest   => quit(q)
            case a: AuthRequest   => auth(a)
            case p: PingRequest   => ping(p)
            case e: EchoRequest   => echo(e)
            case s: SelectRequest => select(s)
            case s: SwapDBRequest => fail(out, new UnsupportedOperationException("swap is unsupported operation."))
            // --- transactions
            case m: MultiRequest   => multi(m)
            case e: ExecRequest    => exec(e)
            case d: DiscardRequest => dicard(d)
            case u: UnwatchRequest => unwatch(u)
            case w: WatchRequest   => watch(w)
            // --- strings
            case a: AppendRequest      => append(a)
            case bc: BitCountRequest   => bitCount(bc)
            case bf: BitFieldRequest   => bitField(bf)
            case bo: BitOpRequest      => bitOp(bo)
            case bp: BitPosRequest     => bitPos(bp)
            case d: DecrRequest        => decr(d)
            case d: DecrByRequest      => decrBy(d)
            case g: GetRequest         => get(g)
            case g: GetBitRequest      => getBit(g)
            case gr: GetRangeRequest   => getRange(gr)
            case gs: GetSetRequest     => getSet(gs)
            case i: IncrRequest        => incr(i)
            case i: IncrByRequest      => incrBy(i)
            case i: IncrByFloatRequest => incrByFloat(i)
            case mg: MGetRequest       => mGet(mg)
            case ms: MSetRequest       => mSet(ms)
            case ms: MSetNxRequest     => mSetNx(ms)
            case p: PSetExRequest      => pSetEx(p)
            case s: SetRequest         => set(s)
            case s: SetBitRequest      => setBit(s)
            case s: SetExRequest       => setEx(s)
            case s: SetNxRequest       => setNx(s)
            case s: SetRangeRequest    => setRange(s)
            case s: StrLenRequest      => strLen(s)
            // --- Keys
            case d: DelRequest       => del(d)
            case d: DumpRequest      => dump(d)
            case e: ExistsRequest    => exists(e)
            case e: ExpireRequest    => expire(e)
            case e: ExpireAtRequest  => expireAt(e)
            case k: KeysRequest      => keys(k)
            case m: MigrateRequest   => migrate(m)
            case m: MoveRequest      => move(m)
            case p: PersistRequest   => persist(p)
            case p: PExpireRequest   => pexpire(p)
            case p: PExpireAtRequest => pexpireAt(p)
            case p: PTtlRequest      => pttl(p)
            case r: RandomKeyRequest =>
              transaction match {
                case Some(tc) =>
                  run(r)(tc.randomKey()) { result =>
                    txState.append(ResponseF[String](result, { p =>
                      RandomKeySucceeded(UUID.randomUUID(), r.id, Option(p.get))
                    }))
                    RandomKeySuspended(UUID.randomUUID(), r.id)
                  } { t =>
                    RandomKeyFailed(UUID.randomUUID(), r.id, RedisIOException(Some(t.getMessage), Some(t)))
                  }()
                case None =>
                  run(r)(jedis.randomKey()) { result =>
                    RandomKeySucceeded(UUID.randomUUID(), r.id, Option(result))
                  } { t =>
                    RandomKeyFailed(UUID.randomUUID(), r.id, RedisIOException(Some(t.getMessage), Some(t)))
                  }()
              }
            // -- BLits
            case b: BLPopRequest => blPop(b)
            case b: BRPopRequest => brPop(b)
            // -- Lits
            case l: LPopRequest   => lPop(l)
            case l: LPushRequest  => lPush(l)
            case r: RPushRequest  => rpush(r)
            case l: LRangeRequest => lrange(l)
            case l: LLenRequest   => llen(l)

            // --- Hashes
            case h: HDelRequest    => hDel(h)
            case h: HExistsRequest => hExists(h)
            case h: HGetRequest    => hGet(h)
            case h: HGetAllRequest => hGetAll(h)
            case h: HSetRequest    => hSet(h)
            case h: HSetNxRequest  => hSetNx(h)
            // --- Sets
            case s: SAddRequest => sadd(s)
          }
        }
      }

      private def migrate(m: MigrateRequest) = {
        transaction match {
          case Some(tc) =>
            run(m)(tc.migrate(m.host, m.port, m.key, m.toDbNo, m.timeout.toMillis.toInt)) { result =>
              txState.append(ResponseF[String](result, { p =>
                MigrateSucceeded(UUID.randomUUID(), m.id, Status.withName(p.get))
              }))
              MigrateSuspended(UUID.randomUUID(), m.id)
            } { t =>
              MigrateFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(m)(jedis.migrate(m.host, m.port, m.key, m.toDbNo, m.timeout.toMillis.toInt)) { result =>
              MigrateSucceeded(UUID.randomUUID(), m.id, Status.withName(result))
            } { t =>
              MigrateFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def move(m: MoveRequest) = {
        transaction match {
          case Some(tc) =>
            run(m)(tc.move(m.key, m.db)) { result =>
              txState.append(ResponseF[lang.Long](result, { s =>
                MoveSucceeded(UUID.randomUUID(), m.id, s.get == 1)
              }))
              MoveSuspended(UUID.randomUUID(), m.id)
            } { t =>
              MoveFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(m)(jedis.move(m.key, m.db)) { result =>
              MoveSucceeded(UUID.randomUUID(), m.id, result == 1)
            } { t =>
              MoveFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def persist(p: PersistRequest) = {
        transaction match {
          case Some(tc) =>
            run(p)(tc.persist(p.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { s =>
                PersistSucceeded(UUID.randomUUID(), p.id, s.get == 1)
              }))
              PersistSuspended(UUID.randomUUID(), p.id)
            } { t =>
              PersistFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.persist(p.key)) { result =>
              PersistSucceeded(UUID.randomUUID(), p.id, result == 1)
            } { t =>
              PersistFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def pttl(p: PTtlRequest) = {
        transaction match {
          case Some(tc) =>
            run(p)(tc.pttl(p.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { s =>
                PTtlSucceeded(UUID.randomUUID(), p.id, s.get)
              }))
              PTtlSuspended(UUID.randomUUID(), p.id)
            } { t =>
              PTtlFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.pttl(p.key)) { result =>
              PTtlSucceeded(UUID.randomUUID(), p.id, result)
            } { t =>
              PTtlFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def sadd(s: SAddRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(s)(tc.sadd(s.key, s.values.toList: _*)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                SAddSucceeded(UUID.randomUUID(), s.id, p.get)
              }))
              SAddSuspended(UUID.randomUUID(), s.id)
            } { t =>
              SAddFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(s)(jedis.sadd(s.key, s.values.toList: _*)) { result =>
              SAddSucceeded(UUID.randomUUID(), s.id, result)
            } { t =>
              SAddFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def llen(l: LLenRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(l)(tc.llen(l.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                LLenSucceeded(UUID.randomUUID(), l.id, p.get)
              }))
              LLenSuspended(UUID.randomUUID(), l.id)
            } { t =>
              LLenFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(l)(jedis.llen(l.key)) { result =>
              LLenSucceeded(UUID.randomUUID(), l.id, result)
            } { t =>
              LLenFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }
      private def pexpireAt(p: PExpireAtRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(p)(tc.pexpireAt(p.key, p.millisecondsTimestamp.toInstant.toEpochMilli)) { result =>
              txState.append(ResponseF[lang.Long](result, { s =>
                PExpireAtSucceeded(UUID.randomUUID(), p.id, s.get == 1)
              }))
              PExpireAtSuspended(UUID.randomUUID(), p.id)
            } { t =>
              PExpireAtFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.pexpireAt(p.key, p.millisecondsTimestamp.toInstant.toEpochMilli)) { result =>
              PExpireAtSucceeded(UUID.randomUUID(), p.id, result == 1)
            } { t =>
              PExpireAtFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def lrange(l: LRangeRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(l)(tc.lrange(l.key, l.start, l.stop)) { result =>
              txState.append(ResponseF[util.List[String]](result, { p =>
                LRangeSucceeded(UUID.randomUUID(), l.id, p.get.asScala)
              }))
              LRangeSuspended(UUID.randomUUID(), l.id)
            } { t =>
              LRangeFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(l)(jedis.lrange(l.key, l.start, l.stop)) { result =>
              LRangeSucceeded(UUID.randomUUID(), l.id, result.asScala)
            } { t =>
              LRangeFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def rpush(r: RPushRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(r)(tc.rpush(r.key, r.values.toList: _*)) { result =>
              txState.append(ResponseF[java.lang.Long](result, { p =>
                RPushSucceeded(UUID.randomUUID(), r.id, p.get)
              }))
              RPushSuspend(UUID.randomUUID(), r.id)
            } { t =>
              RPushFailed(UUID.randomUUID(), r.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(r)(jedis.rpush(r.key, r.values.toList: _*)) { result =>
              RPushSucceeded(UUID.randomUUID(), r.id, result)
            } { t =>
              RPushFailed(UUID.randomUUID(), r.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def pexpire(p: PExpireRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(p)(tc.pexpire(p.key, p.milliseconds.toMillis)) { result =>
              txState.append(ResponseF[lang.Long](result, { s =>
                PExpireSucceeded(UUID.randomUUID(), p.id, s.get == 1)
              }))
              PExpireSuspended(UUID.randomUUID(), p.id)
            } { t =>
              PExpireFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(p)(jedis.pexpire(p.key, p.milliseconds.toMillis)) { result =>
              PExpireSucceeded(UUID.randomUUID(), p.id, result == 1)
            } { t =>
              PExpireFailed(UUID.randomUUID(), p.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def select(s: SelectRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(s)(tc.select(s.index)) { result =>
              txState.append(ResponseF[String](result, { _ =>
                SelectSucceeded(UUID.randomUUID(), s.id)
              }))
              SelectSuspended(UUID.randomUUID(), s.id)
            } { t =>
              SelectFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(s)(jedis.select(s.index)) { _ =>
              SelectSucceeded(UUID.randomUUID(), s.id)
            } { t =>
              SelectFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def expireAt(e: ExpireAtRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(e)(tc.expireAt(e.key, e.expiresAt.toInstant.getEpochSecond)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                ExpireAtSucceeded(UUID.randomUUID(), e.id, p.get == 1)
              }))
              ExpireAtSuspended(UUID.randomUUID(), e.id)
            } { t =>
              ExpireAtFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(e)(jedis.expireAt(e.key, e.expiresAt.toInstant.getEpochSecond)) { result =>
              ExpireAtSucceeded(UUID.randomUUID(), e.id, result == 1)
            } { t =>
              ExpireAtFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def dump(d: DumpRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(d)(tc.dump(d.key)) { result =>
              txState.append(ResponseF[Array[Byte]](result, { p =>
                DumpSucceeded(UUID.randomUUID(), d.id, Option(p.get))
              }))
              DumpSuspended(UUID.randomUUID(), d.id)
            } { t =>
              DumpFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(d)(jedis.dump(d.key)) { result =>
              DumpSucceeded(UUID.randomUUID(), d.id, Option(result))
            } { t =>
              DumpFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def expire(e: ExpireRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(e)(tc.expire(e.key, e.seconds.toSeconds.toInt)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                ExpireSucceeded(UUID.randomUUID(), e.id, p.get == 1)
              }))
              ExpireSuspended(UUID.randomUUID(), e.id)
            } { t =>
              ExpireFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(e)(jedis.expire(e.key, e.seconds.toSeconds.toInt)) { result =>
              ExpireSucceeded(UUID.randomUUID(), e.id, result == 1)
            } { t =>
              ExpireFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def exists(e: ExistsRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(e)(tc.exists(e.keys.toList: _*)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                ExistsSucceeded(UUID.randomUUID(), e.id, p.get == 1L)
              }))
              ExistsSuspended(UUID.randomUUID(), e.id)
            } { t =>
              ExistsFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(e)(jedis.exists(e.keys.toList: _*)) { result =>
              ExistsSucceeded(UUID.randomUUID(), e.id, result == 1L)
            } { t =>
              ExistsFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def del(d: DelRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(d)(tc.del(d.keys.toList: _*)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                DelSucceeded(UUID.randomUUID(), d.id, p.get)
              }))
              DelSuspended(UUID.randomUUID(), d.id)
            } { t =>
              DelFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(d)(jedis.del(d.keys.toList: _*)) { result =>
              DelSucceeded(UUID.randomUUID(), d.id, result)
            } { t =>
              DelFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def unwatch(u: UnwatchRequest): Future[CommandResponse] = {
        run(u)(jedis.unwatch()) { result =>
          WatchSucceeded(UUID.randomUUID(), u.id)
        } { t =>
          WatchFailed(UUID.randomUUID(), u.id, RedisIOException(Some(t.getMessage), Some(t)))
        }()
      }

      private def watch(w: WatchRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(w)(tc.watch(w.keys.toList: _*)) { result =>
              txState.append(ResponseF[String](result, { _ =>
                WatchSucceeded(UUID.randomUUID(), w.id)
              }))
              WatchSuspended(UUID.randomUUID(), w.id)
            } { t =>
              WatchFailed(UUID.randomUUID(), w.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(w)(jedis.watch(w.keys.toList: _*)) { result =>
              WatchSucceeded(UUID.randomUUID(), w.id)
            } { t =>
              WatchFailed(UUID.randomUUID(), w.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def mSetNx(ms: MSetNxRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(ms)(tc.msetnx(ms.values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }: _*)) { result =>
              txState.append(
                ResponseF[java.lang.Long](result, { p =>
                  MSetNxSucceeded(UUID.randomUUID(), ms.id, p.get == 1L)
                })
              )
              MSetNxSuspended(UUID.randomUUID(), ms.id)
            } { t =>
              MSetNxFailed(UUID.randomUUID(), ms.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(ms)(jedis.msetnx(ms.values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }: _*)) { result =>
              MSetNxSucceeded(UUID.randomUUID(), ms.id, result == 1L)
            } { t =>
              MSetNxFailed(UUID.randomUUID(), ms.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def mSet(ms: MSetRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            val args = ms.values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }
            run(ms)(tc.mset(args: _*)) { result =>
              txState.append(ResponseF[String](result, { p =>
                MSetSucceeded(UUID.randomUUID(), ms.id)
              }))
              MSetSuspended(UUID.randomUUID(), ms.id)
            } { t =>
              MSetFailed(UUID.randomUUID(), ms.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            val args = ms.values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }
            run(ms)(jedis.mset(args: _*)) { result =>
              MSetSucceeded(UUID.randomUUID(), ms.id)
            } { t =>
              MSetFailed(UUID.randomUUID(), ms.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def mGet(mg: MGetRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(mg)(tc.mget(mg.keys.toList: _*)) { result =>
              txState.append(
                ResponseF[util.List[String]](result, { p =>
                  MGetSucceeded(UUID.randomUUID(), mg.id, result.get.asScala)
                })
              )
              MGetSuspended(UUID.randomUUID(), mg.id)
            } { t =>
              MGetFailed(UUID.randomUUID(), mg.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(mg)(jedis.mget(mg.keys.toList: _*)) { result =>
              MGetSucceeded(UUID.randomUUID(), mg.id, result.asScala)
            } { t =>
              MGetFailed(UUID.randomUUID(), mg.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def incrByFloat(i: IncrByFloatRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(i)(tc.incrByFloat(i.key, i.value)) { result =>
              txState.append(ResponseF[lang.Double](result, { p =>
                IncrByFloatSucceeded(UUID.randomUUID(), i.id, p.get)
              }))
              IncrByFloatSuspended(UUID.randomUUID(), i.id)
            } { t =>
              IncrByFloatFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(i)(jedis.incrByFloat(i.key, i.value)) { result =>
              IncrByFloatSucceeded(UUID.randomUUID(), i.id, result)
            } { t =>
              IncrByFloatFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def incrBy(i: IncrByRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(i)(tc.incrBy(i.key, i.value)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                IncrBySucceeded(UUID.randomUUID(), i.id, p.get)
              }))
              IncrBySuspended(UUID.randomUUID(), i.id)
            } { t =>
              IncrByFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(i)(jedis.incrBy(i.key, i.value)) { result =>
              IncrBySucceeded(UUID.randomUUID(), i.id, result)
            } { t =>
              IncrByFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def setRange(s: SetRangeRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(s)(tc.setrange(s.key, s.range, s.value)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                SetRangeSucceeded(UUID.randomUUID(), s.id, p.get)
              }))
              SetRangeSuspended(UUID.randomUUID(), s.id)
            } { t =>
              SetRangeFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(s)(jedis.setrange(s.key, s.range, s.value)) { result =>
              SetRangeSucceeded(UUID.randomUUID(), s.id, result)
            } { t =>
              SetRangeFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def lPush(l: LPushRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(l)(tc.lpush(l.key, l.values.toList: _*)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                LPushSucceeded(UUID.randomUUID(), l.id, p.get)
              }))
              LPushSuspended(UUID.randomUUID(), l.id)
            } { t =>
              LPushFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(l)(jedis.lpush(l.key, l.values.toList: _*)) { result =>
              LPushSucceeded(UUID.randomUUID(), l.id, result)
            } { t =>
              LPushFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def lPop(l: LPopRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(l)(tc.lpop(l.key)) { result =>
              txState.append(ResponseF[String](result, { p =>
                LPopSucceeded(UUID.randomUUID(), l.id, Option(p.get))
              }))
              LPopSuspended(UUID.randomUUID(), l.id)
            } { t =>
              LPopFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(l)(jedis.lpop(l.key)) { result =>
              LPopSucceeded(UUID.randomUUID(), l.id, Option(result))
            } { t =>
              LPopFailed(UUID.randomUUID(), l.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hSetNx(h: HSetNxRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hsetnx(h.key, h.field, h.value)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                HSetNxSucceeded(UUID.randomUUID(), h.id, p.get == 1L)
              }))
              HSetNxSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HSetNxFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hsetnx(h.key, h.field, h.value)) { result =>
              HSetNxSucceeded(UUID.randomUUID(), h.id, result == 1L)
            } { t =>
              HSetNxFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hSet(h: HSetRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hset(h.key, h.field, h.value)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                HSetSucceeded(UUID.randomUUID(), h.id, p.get == 1L)
              }))
              HSetSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HSetFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hset(h.key, h.field, h.value)) { result =>
              HSetSucceeded(UUID.randomUUID(), h.id, result == 1L)
            } { t =>
              HSetFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hGetAll(h: HGetAllRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hgetAll(h.key)) { result =>
              txState.append(ResponseF[util.Map[String, String]](result, { p =>
                HGetAllSucceeded(UUID.randomUUID(), h.id, p.get.asScala.toSeq.flatMap(v => Seq(v._1, v._2)))
              }))
              HGetAllSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HGetAllFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hgetAll(h.key)) { result =>
              HGetAllSucceeded(UUID.randomUUID(), h.id, result.asScala.toSeq.flatMap(v => Seq(v._1, v._2)))
            } { t =>
              HGetAllFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hGet(h: HGetRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hget(h.key, h.field)) { result =>
              txState.append(ResponseF[String](result, { p =>
                HGetSucceeded(UUID.randomUUID(), h.id, Option(p.get))
              }))
              HGetSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HGetFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hget(h.key, h.field)) { result =>
              HGetSucceeded(UUID.randomUUID(), h.id, Option(result))
            } { t =>
              HGetFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def brPop(b: BRPopRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            def command =
              if (b.timeout.isFinite())
                tc.brpop(b.timeout.toSeconds.toInt, b.keys.toList: _*)
              else
                tc.brpop(b.keys.toList: _*)

            run(b)(command) { result =>
              txState.append(
                ResponseF[util.List[String]](result, { p =>
                  BRPopSucceeded(UUID.randomUUID(), b.id, p.get.asScala)
                })
              )
              BRPopSuspended(UUID.randomUUID(), b.id)
            } { t =>
              BRPopFailed(UUID.randomUUID(), b.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            def command =
              if (b.timeout.isFinite())
                jedis.brpop(b.timeout.toSeconds.toInt, b.keys.toList: _*)
              else
                jedis.brpop(b.keys.toList: _*)

            run(b)(command) { result =>
              BRPopSucceeded(UUID.randomUUID(), b.id, result.asScala)
            } { t =>
              BRPopFailed(UUID.randomUUID(), b.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hExists(h: HExistsRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hexists(h.key, h.field)) { result =>
              txState.append(ResponseF[lang.Boolean](result, { p =>
                HExistsSucceeded(UUID.randomUUID(), h.id, p.get)
              }))
              HExistsSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HExistsFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hexists(h.key, h.field)) { result =>
              HExistsSucceeded(UUID.randomUUID(), h.id, result)
            } { t =>
              HExistsFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def hDel(h: HDelRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(h)(tc.hdel(h.key, h.fields.toList: _*)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                HDelSucceeded(UUID.randomUUID(), h.id, result.get)
              }))
              HDelSuspended(UUID.randomUUID(), h.id)
            } { t =>
              HDelFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(h)(jedis.hdel(h.key, h.fields.toList: _*)) { result =>
              HDelSucceeded(UUID.randomUUID(), h.id, result)
            } { t =>
              HDelFailed(UUID.randomUUID(), h.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def blPop(b: BLPopRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            def command =
              if (b.timeout.isFinite())
                tc.blpop(b.timeout.toSeconds.toInt, b.keys.toList: _*)
              else
                tc.blpop(b.keys.toList: _*)

            run(b)(command) { result =>
              txState.append(
                ResponseF[util.List[String]](result, { p =>
                  BLPopSucceeded(UUID.randomUUID(), b.id, p.get.asScala)
                })
              )
              BLPopSuspended(UUID.randomUUID(), b.id)
            } { t =>
              BLPopFailed(UUID.randomUUID(), b.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            def command =
              if (b.timeout.isFinite())
                jedis.blpop(b.timeout.toSeconds.toInt, b.keys.toList: _*)
              else
                jedis.blpop(b.keys.toList: _*)

            run(b)(command) { result =>
              BLPopSucceeded(UUID.randomUUID(), b.id, result.asScala)
            } { t =>
              BLPopFailed(UUID.randomUUID(), b.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def strLen(s: StrLenRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(s)(tc.strlen(s.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                StrLenSucceeded(UUID.randomUUID(), s.id, p.get)
              }))
              StrLenSuspended(UUID.randomUUID(), s.id)
            } { t =>
              StrLenFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(s)(jedis.strlen(s.key)) { result =>
              StrLenSucceeded(UUID.randomUUID(), s.id, result)
            } { t =>
              StrLenFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def getSet(gs: GetSetRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(gs)(tc.getSet(gs.key, gs.value)) { result =>
              txState.append(ResponseF[String](result, { p =>
                GetSetSucceeded(UUID.randomUUID(), gs.id, Option(p.get))
              }))
              GetSetSuspended(UUID.randomUUID(), gs.id)
            } { t =>
              GetSetFailed(UUID.randomUUID(), gs.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(gs)(jedis.getSet(gs.key, gs.value)) { result =>
              GetSetSucceeded(UUID.randomUUID(), gs.id, Option(result))
            } { t =>
              GetSetFailed(UUID.randomUUID(), gs.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def incr(i: IncrRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(i)(tc.incr(i.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                IncrSucceeded(UUID.randomUUID(), i.id, p.get)
              }))
              IncrSuspended(UUID.randomUUID(), i.id)
            } { t =>
              IncrFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(i)(jedis.incr(i.key)) { result =>
              IncrSucceeded(UUID.randomUUID(), i.id, result)
            } { t =>
              IncrFailed(UUID.randomUUID(), i.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def getRange(gr: GetRangeRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(gr)(tc.getrange(gr.key, gr.startAndEnd.start, gr.startAndEnd.end)) { result =>
              txState.append(ResponseF[String](result, { p =>
                GetRangeSucceeded(UUID.randomUUID(), gr.id, Option(p.get))
              }))
              GetRangeSuspended(UUID.randomUUID(), gr.id)
            } { t =>
              GetRangeFailed(UUID.randomUUID(), gr.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(gr)(jedis.getrange(gr.key, gr.startAndEnd.start, gr.startAndEnd.end)) { result =>
              GetRangeSucceeded(UUID.randomUUID(), gr.id, Option(result))
            } { t =>
              GetRangeFailed(UUID.randomUUID(), gr.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def decrBy(d: DecrByRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(d)(tc.decrBy(d.key, d.value)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                DecrBySucceeded(UUID.randomUUID(), d.id, p.get)
              }))
              DecrBySuspended(UUID.randomUUID(), d.id)
            } { t =>
              DecrByFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(d)(jedis.decrBy(d.key, d.value)) { result =>
              DecrBySucceeded(UUID.randomUUID(), d.id, result)
            } { t =>
              DecrByFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def setBit(s: SetBitRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(s)(tc.setbit(s.key, s.offset, if (s.value == 1L) true else false)) { result =>
              txState.append(
                ResponseF[lang.Boolean](result, { p =>
                  SetBitSucceeded(UUID.randomUUID(), s.id, if (p.get) 1 else 0)
                })
              )
              SetBitSuspended(UUID.randomUUID(), s.id)
            } { t =>
              SetBitFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(s)(jedis.setbit(s.key, s.offset, if (s.value == 1L) true else false)) { result =>
              SetBitSucceeded(UUID.randomUUID(), s.id, if (result) 1 else 0)
            } { t =>
              SetBitFailed(UUID.randomUUID(), s.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def getBit(g: GetBitRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(g)(tc.getbit(g.key, g.offset)) { result =>
              txState.append(
                ResponseF[lang.Boolean](result, { p =>
                  GetBitSucceeded(UUID.randomUUID(), g.id, if (p.get) 1 else 0)
                })
              )
              GetBitSuspended(UUID.randomUUID(), g.id)
            } { t =>
              GetBitFailed(UUID.randomUUID(), g.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(g)(jedis.getbit(g.key, g.offset)) { result =>
              GetBitSucceeded(UUID.randomUUID(), g.id, if (result) 1 else 0)
            } { t =>
              GetBitFailed(UUID.randomUUID(), g.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def decr(d: DecrRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(d)(tc.decr(d.key)) { result =>
              txState.append(ResponseF[lang.Long](result, { p =>
                DecrSucceeded(UUID.randomUUID(), d.id, p.get)
              }))
              DecrSuspended(UUID.randomUUID(), d.id)
            } { t =>
              DecrFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(d)(jedis.decr(d.key)) { result =>
              DecrSucceeded(UUID.randomUUID(), d.id, result)
            } { t =>
              DecrFailed(UUID.randomUUID(), d.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def echo(e: EchoRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(e)(tc.echo(e.message)) { result =>
              txState.append(ResponseF[String](result, { p =>
                EchoSucceeded(UUID.randomUUID(), e.id, p.get)
              }))
              EchoSuspended(UUID.randomUUID(), e.id)
            } { t =>
              EchoFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(e)(jedis.echo(e.message)) { result =>
              EchoSucceeded(UUID.randomUUID(), e.id, result)
            } { t =>
              EchoFailed(UUID.randomUUID(), e.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def auth(a: AuthRequest): Future[CommandResponse] = {
        run(a)(jedis.auth(a.password)) { _ =>
          AuthSucceeded(UUID.randomUUID(), a.id)
        } { t =>
          AuthFailed(UUID.randomUUID(), a.id, RedisIOException(Some(t.getMessage), Some(t)))
        }()
      }

      private def quit(q: QuitRequest): Future[CommandResponse] = {
        run(q)(jedis.quit())(result => QuitSucceeded(UUID.randomUUID(), q.id))(
          t => QuitFailed(UUID.randomUUID(), q.id, RedisIOException(Some(t.getMessage), Some(t)))
        )()
      }

      private def bitPos(bp: BitPosRequest): Future[CommandResponse] = {
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

      private def bitOp(bo: BitOpRequest): Future[CommandResponse] = {
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

      private def bitField(bf: BitFieldRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc) =>
            run(bf)(tc.bitfield(bf.key, bf.options.toList.flatMap(v => v.toSeq): _*)) { result =>
              txState.append(ResponseF[util.List[lang.Long]](result, { r =>
                BitFieldSucceeded(UUID.randomUUID(), bf.id, r.get.asScala.map(_.toLong))
              }))
              BitFieldSuspended(UUID.randomUUID(), bf.id)
            } { t =>
              BitFieldFailed(UUID.randomUUID(), bf.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case None =>
            run(bf)(jedis.bitfield(bf.key, bf.options.toList.flatMap(v => v.toSeq): _*)) { result =>
              BitFieldSucceeded(UUID.randomUUID(), bf.id, result.asScala.map(_.toLong))
            } { t =>
              BitFieldFailed(UUID.randomUUID(), bf.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
        }
      }

      private def keys(k: KeysRequest): Future[CommandResponse] = {
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

      private def pSetEx(p: PSetExRequest): Future[CommandResponse] = {
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

      private def exec(e: ExecRequest): Future[CommandResponse] = {
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

      private def append(a: AppendRequest): Future[CommandResponse] = {
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

      private def bitCount(b: BitCountRequest): Future[CommandResponse] = {
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
            } { result =>
              txState.append(ResponseF[java.lang.Long](result, { p =>
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

      private def set(s: SetRequest): Future[CommandResponse] = {
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

      private def setNx(s: SetNxRequest): Future[CommandResponse] = {
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

      private def setEx(s: SetExRequest): Future[CommandResponse] = {
        import s._
        transaction match {
          case Some(tc) =>
            run(s)(tc.setex(key, expires.toSeconds.toInt, value)) { result =>
              txState.append(ResponseF[String](result, { _ =>
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

      private def get(g: GetRequest): Future[CommandResponse] = {
        import g._
        transaction match {
          case Some(tc) =>
            run(g)(tc.get(key)) { result =>
              txState.append(
                ResponseF[String](result, { result =>
                  GetSucceeded(UUID.randomUUID(), id, Option(result.get))
                })
              )
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

      private def multi(m: MultiRequest): Future[CommandResponse] = {
        run(m)(jedis.multi()) { result =>
          transaction = Some(result)
          MultiSucceeded(UUID.randomUUID(), m.id)
        } { t =>
          MultiFailed(UUID.randomUUID(), m.id, RedisIOException(Some(t.getMessage), Some(t)))
        }()
      }

      private def ping(pr: PingRequest): Future[CommandResponse] = {
        transaction match {
          case Some(tc: Transaction with JedisEx[Response]) =>
            run(pr)(tc.pingArg(pr.message)) { r =>
              txState.append(ResponseF[String](r, { r =>
                PingSucceeded(UUID.randomUUID(), pr.id, r.get)
              }))
              PingSuspended(UUID.randomUUID(), pr.id)
            } { t =>
              PingFailed(UUID.randomUUID(), pr.id, RedisIOException(Some(t.getMessage), Some(t)))
            }()
          case Some(_) =>
            throw new AssertionError()
          case None =>
            run(pr)(jedis.pingArg(pr.message)) {
              PingSucceeded(UUID.randomUUID(), pr.id, _)
            } { t =>
              PingFailed(UUID.randomUUID(), pr.id, RedisIOException(Some(t.getMessage), Some(t)))
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
        log.debug("preStart: start")
        completionState = None
        inFlight = 0
        resultCallback = getAsyncCallback[RequestWithResult](handleResult)
        jedis = (connectionTimeout, socketTimeout) match {
          case (Some(ct), None) =>
            new Jedis(host, port, if (ct.isFinite()) ct.toSeconds.toInt else 0) with JedisEx[Id] {
              val custom = new Client(host, port) {
                def pingArg(argOpt: Option[String]): Unit = {
                  argOpt match {
                    case None =>
                      ping()
                    case Some(arg) =>
                      sendCommand(Command.PING, arg.getBytes)
                  }
                }
              }

              override def multi(): Transaction = {
                new Transaction(this.custom) with JedisEx[Response] {
                  override def pingArg(arg: Option[String]): Response[String] = {
                    custom.pingArg(arg)
                    getResponse(BuilderFactory.STRING)
                  }
                }
              }

              def pingArg(arg: Option[String]): Id[String] = {
                custom.pingArg(arg)
                custom.getStatusCodeReply
              }
            }
          case (Some(ct), Some(st)) =>
            new Jedis(host,
                      port,
                      if (ct.isFinite()) ct.toSeconds.toInt else 0,
                      if (st.isFinite()) st.toSeconds.toInt else 0) with JedisEx[Id] {
              val custom = new Client(host, port) {
                def pingArg(argOpt: Option[String]): Unit = {
                  argOpt match {
                    case None =>
                      ping()
                    case Some(arg) =>
                      sendCommand(Command.PING, arg.getBytes)
                  }
                }
              }

              override def multi(): Transaction = {
                new Transaction(this.custom) with JedisEx[Response] {
                  override def pingArg(arg: Option[String]): Response[String] = {
                    custom.pingArg(arg)
                    getResponse(BuilderFactory.STRING)
                  }
                }
              }

              override def pingArg(arg: Option[String]): Id[String] = {
                custom.pingArg(arg)
                custom.getStatusCodeReply
              }
            }
          case _ =>
            new Jedis(host, port) with JedisEx[Id] {
              val custom = new Client(host, port) {
                def pingArg(argOpt: Option[String]): Unit = {
                  argOpt match {
                    case None =>
                      ping()
                    case Some(arg) =>
                      sendCommand(Command.PING, arg.getBytes)

                  }
                }
              }

              override def multi(): Transaction = {
                new Transaction(this.custom) with JedisEx[Response] {
                  override def pingArg(arg: Option[String]): Response[String] = {
                    custom.pingArg(arg)
                    getResponse(BuilderFactory.STRING)
                  }
                }
              }

              def pingArg(arg: Option[String]): Id[String] = {
                custom.pingArg(arg)
                custom.getStatusCodeReply
              }
            }
        }
        promise.success(jedis)
        pull(in)
        log.debug("preStart: finished")
      }

      override def postStop(): Unit = {
        log.debug("postStop: start")
        jedis.close()
        log.debug("postStop: finished")
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

          override def onDownstreamFinish(): Unit = {
            log.debug("onDownstreamFinish")
            completionState = Some(Success(()))
            checkForCompletion()
          }

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
