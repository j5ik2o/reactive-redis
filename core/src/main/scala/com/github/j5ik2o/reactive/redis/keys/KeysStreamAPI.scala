package com.github.j5ik2o.reactive.redis.keys

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait KeysStreamAPI extends BaseStreamAPI {

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

  // --- DUMP

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

  // --- EXPIRE
  private def expireSource(key: String, timeout: Long) = Source.single(s"EXPIRE $key $timeout")

  def expire(key: String, timeout: Long)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
    expireSource(key, timeout).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
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
    expireAtSource(key, unixTime).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
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

  // --- MIGRATE

  // --- MOVE
  private def moveSource(key: String, index: Int) = Source.single(s"MOVE $key $index")

  def move(key: String, index: Int)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    moveSource(key, index).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
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

  // --- OBJECT

  // --- PERSIST
  private def persistSource(key: String) = Source.single(s"PERSIST $key")

  def persist(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Boolean] = {
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

  // --- PEXPIRE

  // --- PEXPIREAT

  // --- PTTL

  // --- RANDOMKEY
  private val randomKeySource: Source[String, NotUsed] = Source.single("RANDOMKEY")

  def randomKey(implicit mat: Materializer, ec: ExecutionContext): Future[Option[String]] = {
    randomKeySource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case dollorRegex(d) =>
          if (d.toInt == -1) {
            None
          } else {
            Some(v(1))
          }
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

  // --- RESTORE

  // --- SORT

  // --- TTL
  private def ttlSource(key: String) = Source.single(s"TTL $key")

  def ttl(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Int] = {
    ttlSource(key).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
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

  // --- WAIT

  // --- SCAN

}
