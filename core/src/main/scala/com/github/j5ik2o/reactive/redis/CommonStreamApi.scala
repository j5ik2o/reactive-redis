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

  protected val toByteString: Flow[String, ByteString, NotUsed] = Flow[String].map{ s => ByteString(s.concat("\r\n")) }

  protected val sink: Sink[ByteString, Future[Seq[String]]] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .log("response")
    .toMat(Sink.fold(Seq.empty[String])((acc, in) => acc :+ in))(Keep.right)

  // --- QUIT

  private val quitSource: Source[String, NotUsed] = Source.single("QUIT")

  def quit(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    quitSource.log("request").via(toByteString).via(connection).map(_ => ()).toMat(Sink.last)(Keep.right).run()
  }

  // --- EXISTS

  private def existsSource(key: String): Source[String, NotUsed] = Source.single(s"EXISTS $key")

  def exists(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
    existsSource(key).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- DEL

  private def delSource(keys: Seq[String]): Source[String, NotUsed] = Source.single(s"DEL ${keys.mkString(" ")}")

  def del(keys: Seq[String])(implicit mat: Materializer, ec: ExecutionContext): Future[Int] = {
    delSource(keys).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(n) =>
          n.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- TYPE

  private def typeSource(key: String): Source[String, NotUsed] = Source.single(s"TYPE $key")

  def `type`(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[ValueType.Value] = {
    typeSource(key).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(s) =>
          ValueType.withName(s)
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- KEYS

  private def keysSource(keyPattern: String) = Source.single(s"KEYS $keyPattern")

  def keys(keyPattern: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[String]] = {
    keysSource(keyPattern).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
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
  }

  // --- RANDOMKEY

  private val randomKeySource: Source[String, NotUsed] = Source.single("RANDOMKEY")

  def randomKey(implicit mat: Materializer, ec: ExecutionContext): Future[String] = {
    randomKeySource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(s) =>
          s
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- RENAME

  private def renameSource(oldKey: String, newKey: String): Source[String, NotUsed] = Source.single(s"RENAME $oldKey $newKey")

  def rename(oldKey: String, newKey: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    renameSource(oldKey, newKey).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(_) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- RENAMENX

  private def renameNxSource(oldKey: String, newKey: String): Source[String, NotUsed] = Source.single(s"RENAMENX $oldKey $newKey")

  def renameNx(oldKey: String, newKey: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
    renameNxSource(oldKey, newKey).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- DBSIZE

  private val dbSizeSource: Source[String, NotUsed] = Source.single("DBSIZE")

  def dbSize(implicit mat: Materializer, ec: ExecutionContext): Future[Int] = {
    dbSizeSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- EXPIRE

  private def expireSource(key: String, timeout: Long) = Source.single(s"EXPIRE $key $timeout")

  def expire(key: String, timeout: Long)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
    expireSource(key, timeout).log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- EXPIREAT

  private def expireAtSource(key: String, unixTime: Long) = Source.single(s"EXPIREAT $key $unixTime")

  def expireAt(key: String, unixTime: Long)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
    expireAtSource(key, unixTime).log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- PERSIST
  private def persistSource(key: String) = Source.single(s"PERSIST $key")

  def persist(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] =  {
    persistSource(key).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt == 1
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- TTL

  private def ttlSource(key: String) = Source.single(s"TTL $key")

  def ttl(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Int] = {
    ttlSource(key).log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- SELECT

  private def selectSource(index: Int) = Source.single(s"SELECT $index")

  def select(index: Int)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    selectSource(index).log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
      v.head match {
        case stringRegex(_) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- MOVE

  private def moveSource(key: String, index: Int) = Source.single(s"MOVE $key $index")

  def move(key: String, index: Int)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    moveSource(key, index).log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- FLUSHDB

  private val flushDBSource: Source[String, NotUsed] = Source.single("FLUSHDB")

  def flushDB(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    flushDBSource.log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
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

  // --- FLUSHALL

  private val flushAllSource: Source[String, NotUsed] = Source.single("FLUSHALL")

  def flushAll(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    flushAllSource.log("request").via(toByteString).via(connection).runWith(sink).map{ v =>
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
      exists(key).map { v =>
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
    case RenameRequest(oldKey, newKey) =>
      rename(oldKey, newKey).map { _ =>
        RenameSucceeded
      }.recover { case ex: Exception =>
        RenameFailure(ex)
      }.pipeTo(sender())
    case RenameNxRequest(oldKey, newKey) =>
      renameNx(oldKey, newKey).map { v =>
        RenameNxSucceeded(v)
      }.recover { case ex: Exception =>
        RenameNxFailure(ex)
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
    case PersistRequest(key) =>
      persist(key).map { v =>
        PersistSucceeded(v)
      }.recover { case ex: Exception =>
        PersistFailure(ex)
      }.pipeTo(sender())
    case TTLRequest(key) =>
      ttl(key).map { v =>
        TTLSucceeded(v)
      }.recover { case ex: Exception =>
        TTLFailure(ex)
      }.pipeTo(sender())
    case SelectRequest(index) =>
      select(index).map { _ =>
        SelectSucceeded
      }.recover { case ex: Exception =>
        SelectFailure(ex)
      }.pipeTo(sender())
    case MoveRequest(key, index) =>
      move(key, index).map { v =>
        MoveSucceeded
      }.recover { case ex: Exception =>
        MoveFailure(ex)
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
