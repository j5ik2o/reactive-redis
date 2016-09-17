package com.github.j5ik2o.reactive.redis

import java.text.ParseException

import akka.NotUsed
import akka.actor.Actor
import akka.pattern.pipe
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Framing, Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.ByteString

import scala.concurrent.{ ExecutionContext, Future }

trait CommonStreamApi {
  val digitsRegex = """:(\d+)$""".r
  val stringRegex = """\+(\w+)$""".r
  val errorRegex = """\-(\w+)$""".r
  val dollorRegex = """\$(\d+)$""".r
  val listSizeRegex = """\*(\d+)$""".r

  def parseException = new ParseException("protocol parse error", 0)

  protected val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]]

  protected val sink: Sink[ByteString, Future[Seq[String]]] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .toMat(Sink.fold(Seq.empty[String])((acc, in) => acc :+ in))(Keep.right)

  // --- QUIT

  private val quitSource: Source[ByteString, NotUsed] = Source.single(ByteString("QUIT\r\n"))

  def quit(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] =
    connection.runWith(quitSource, Sink.head)._2.map(_ => ())

  // --- EXISTS

  private val existsSource: Source[ByteString, NotUsed] = Source.single(ByteString("EXISTS\r\n"))

  def exists(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(existsSource, sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- DEL

  private def delSource(keys: Seq[String]) = Source.single(ByteString(s"DEL ${keys.mkString(" ")}\r\n"))

  def del(keys: Seq[String])(implicit mat: Materializer, ec: ExecutionContext): Future[Int] =
    connection.runWith(delSource(keys), sink)._2.map { v =>
      v.head match {
        case digitsRegex(n) =>
          n.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }

    }

  // --- TYPE

  private def typeSource(key: String): Source[ByteString, NotUsed] = Source.single(ByteString(s"TYPE $key\r\n"))

  def `type`(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[ValueType.Value] =
    connection.runWith(typeSource(key), sink)._2.map { v =>
      v.head match {
        case stringRegex(s) =>
          ValueType.withName(s)
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- KEYS

  private def keysSource(keyPattern: String) = Source.single(ByteString(s"KEYS $keyPattern\r\n"))

  def keys(keyPattern: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[String]] =
    connection.runWith(keysSource(keyPattern), sink)._2.map { v =>
      v.head match {
        case listSizeRegex(size) =>
          v.tail.filterNot {
            case dollorRegex(_) => true
            case _ => false
          }
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- RANDOMKEY

  private val randomKeySource: Source[ByteString, NotUsed] = Source.single(ByteString("RANDOMKEY\r\n"))

  def randomKey(implicit mat: Materializer, ec: ExecutionContext): Future[String] =
    connection.runWith(randomKeySource, sink)._2.map { v =>
      v.head match {
        case stringRegex(s) =>
          s
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- RENAME

  private def renameSource(oldKey: String, newKey: String): Source[ByteString, NotUsed] = Source.single(ByteString(s"RENAME $oldKey $newKey\r\n"))

  def rename(oldKey: String, newKey: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(renameSource(oldKey, newKey), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- RENAMENX

  private def renameNxSource(oldKey: String, newKey: String): Source[ByteString, NotUsed] = Source.single(ByteString(s"RENAMENX $oldKey $newKey\r\n"))

  def rename(oldKey: String, newKey: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(renameNxSource(oldKey, newKey), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- DBSIZE

  private val dbSizeSource: Source[ByteString, NotUsed] = Source.single(ByteString("DBSIZE\r\n"))

  def dbSize(implicit mat: Materializer, ec: ExecutionContext): Future[Int] =
    connection.runWith(dbSizeSource, sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- EXPIRE

  private def expireSource(key: String, timeout: Long) = Source.single(ByteString(s"EXPIRE $key $timeout\r\n"))

  def expire(key: String, timeout: Long)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(expireSource(key, timeout), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- EXPIREAT

  private def expireAtSource(key: String, unixTime: Long) = Source.single(ByteString(s"EXPIREAT $key $unixTime\r\n"))

  def expireAt(key: String, unixTime: Long)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(expireAtSource(key, unixTime), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- PERSIST
  private def persistSource(key: String) = Source.single(ByteString(s"PERSIST $key\r\n"))

  def persist(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =
    connection.runWith(persistSource(key), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- TTL
  private def ttlSource(key: String) = Source.single(ByteString(s"TTL $key\r\n"))

  def ttl(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Int] =
    connection.runWith(persistSource(key), sink)._2.map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- SELECT

  // --- MOVE

  // --- FLUSHDB

  private val flushDBSource: Source[ByteString, NotUsed] = Source.single(ByteString("FLUSHDB\r\n"))

  def flushDB(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] =
    connection.runWith(flushDBSource, sink)._2.map { v =>
      v.head match {
        case stringRegex(s) =>
          require(s == "OK")
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

  // --- FLUSHALL

  private val flushAllSource: Source[ByteString, NotUsed] = Source.single(ByteString("FLUSHALL\r\n"))

  def flushAll(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] =
    connection.runWith(flushAllSource, sink)._2.map { v =>
      v.head match {
        case stringRegex(s) =>
          require(s == "OK")
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }

}

trait CommonStreamActor extends CommonStreamApi {
  this: Actor =>

  import CommonProtocol._

  implicit val materializer = ActorMaterializer()

  implicit val system = context.system

  import context.dispatcher

  val handleBase: Receive = {
    case QuitRequest =>
      quit.map { v =>
        QuitSucceeded
      }.pipeTo(sender())
    case ExistsRequest(key) =>
      exists.map { v =>
        ExistsSucceeded(v)
      }.recover { case ex: Exception =>
        ExistsFailure(ex)
      }.pipeTo(sender())
    case DelRequest(keys) =>
      del(keys).map { v =>
        DelSucceeded(v)
      }.recover { case ex: Exception =>
        DelFailure(ex)
      }.pipeTo(sender())
    case TypeRequest(key) =>
      `type`(key).map { v =>
        TypeSucceeded(v)
      }.recover { case ex: Exception =>
        TypeFailure(ex)
      }.pipeTo(sender())
    case KeysRequest(keyPattern) =>
      keys(keyPattern).map { v =>
        KeysSucceeded(v)
      }.recover { case ex: Exception =>
        KeysFailure(ex)
      }.pipeTo(sender())
    case RandomKeyRequest =>
      randomKey.map { v =>
        RandomKeySucceeded
      }.recover { case ex: Exception =>
        RandomKeyFailure(ex)
      }.pipeTo(sender())
    case DBSizeRequest =>
      dbSize.map { v =>
        DBSizeSucceeded(v)
      }.recover { case ex: Exception =>
        DBSizeFailure(ex)
      }.pipeTo(sender())
    case ExpireRequest(key, timeout) =>
      expire(key, timeout).map { v =>
        ExpireSucceeded(v)
      }.recover { case ex: Exception =>
        ExpireFailure(ex)
      }.pipeTo(sender())
    case ExpireAtRequest(key, unixTimeout) =>
      expireAt(key, unixTimeout).map { v =>
        ExpireAtSucceeded(v)
      }.recover { case ex: Exception =>
        ExpireAtFailure(ex)
      }.pipeTo(sender())
    case FlushDBRequest =>
      flushDB.map { v =>
        FlushDBSucceeded
      }.recover { case ex: Exception =>
        FlushDBFailure(ex)
      }.pipeTo(sender())
    case FlushAllRequest =>
      flushAll.map { v =>
        FlushAllSucceeded
      }.recover { case ex: Exception =>
        FlushAllFailure(ex)
      }.pipeTo(sender())
  }


}
