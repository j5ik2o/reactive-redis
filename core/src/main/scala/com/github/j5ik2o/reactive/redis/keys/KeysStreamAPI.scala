package com.github.j5ik2o.reactive.redis.keys

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.reactive.redis.keys.KeysProtocol._
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait KeysStreamAPI extends BaseStreamAPI {

  import com.github.j5ik2o.reactive.redis.ResponseRegexs._

  // --- DEL
  def del(keys: Seq[String]): Source[DelRequest, NotUsed] = Source.single(DelRequest(keys))

  // --- DUMP
  def dump(key: String): Source[DumpRequest, NotUsed] = Source.single(DumpRequest(key))

  // --- EXISTS
  def exists(key: String): Source[ExistsRequest, NotUsed] = Source.single(ExistsRequest(key))

  // --- EXPIRE
  def expire(key: String, timeout: Long): Source[ExpireRequest, NotUsed] = Source.single(ExpireRequest(key, timeout))

  // --- EXPIREAT
  def expireAt(key: String, unixTime: Long): Source[ExpireAtRequest, NotUsed] = Source.single(ExpireAtRequest(key, unixTime))

  // --- KEYS
  def keys(keyPattern: String): Source[KeysRequest, NotUsed] = Source.single(KeysRequest(keyPattern))

  // --- MIGRATE

  // --- MOVE
  def move(key: String, index: Int): Source[MoveRequest, NotUsed] = Source.single(MoveRequest(key, index))

  // --- OBJECT

  // --- PERSIST
  def persist(key: String): Source[PersistRequest, NotUsed] = Source.single(PersistRequest(key))

  // --- PEXPIRE

  // --- PEXPIREAT

  // --- PTTL

  // --- RANDOMKEY
  val randomKey: Source[RandomKeyRequest.type, NotUsed] = Source.single(RandomKeyRequest)

  // --- RENAME
  def rename(oldKey: String, newKey: String): Source[RenameRequest, NotUsed] = Source.single(RenameRequest(oldKey, newKey))

  // --- RENAMENX
  def renameNx(oldKey: String, newKey: String): Source[RenameNxRequest, NotUsed] = Source.single(RenameNxRequest(oldKey, newKey))

  // --- RESTORE

  // --- SORT

  // --- TTL
  def ttl(key: String): Source[TTLRequest, NotUsed] = Source.single(TTLRequest(key))

  // --- TYPE
  def `type`(key: String): Source[TypeRequest, NotUsed] = Source.single(TypeRequest(key))

  // --- WAIT

  // --- SCAN

}
