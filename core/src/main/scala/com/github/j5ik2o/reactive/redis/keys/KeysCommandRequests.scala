package com.github.j5ik2o.reactive.redis.keys

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.keys.KeysProtocol._

trait KeysCommandRequests {

  // --- DEL
  def delRequest(keys: Seq[String]): Source[DelRequest, NotUsed] = Source.single(DelRequest(keys))

  // --- DUMP
  def dumpRequest(key: String): Source[DumpRequest, NotUsed] = Source.single(DumpRequest(key))

  // --- EXISTS
  def existsRequest(key: String): Source[ExistsRequest, NotUsed] = Source.single(ExistsRequest(key))

  // --- EXPIRE
  def expireRequest(key: String, timeout: Long): Source[ExpireRequest, NotUsed] = Source.single(ExpireRequest(key, timeout))

  // --- EXPIREAT
  def expireAtRequest(key: String, unixTime: Long): Source[ExpireAtRequest, NotUsed] = Source.single(ExpireAtRequest(key, unixTime))

  // --- KEYS
  def keysRequest(keyPattern: String): Source[KeysRequest, NotUsed] = Source.single(KeysRequest(keyPattern))

  // --- MIGRATE

  // --- MOVE
  def moveRequest(key: String, index: Int): Source[MoveRequest, NotUsed] = Source.single(MoveRequest(key, index))

  // --- OBJECT

  // --- PERSIST
  def persistRequest(key: String): Source[PersistRequest, NotUsed] = Source.single(PersistRequest(key))

  // --- PEXPIRE

  // --- PEXPIREAT

  // --- PTTL

  // --- RANDOMKEY
  val randomKeyRequest: Source[RandomKeyRequest, NotUsed] = Source.single(RandomKeyRequest())

  // --- RENAME
  def renameRequest(oldKey: String, newKey: String): Source[RenameRequest, NotUsed] = Source.single(RenameRequest(oldKey, newKey))

  // --- RENAMENX
  def renameNxRequest(oldKey: String, newKey: String): Source[RenameNxRequest, NotUsed] = Source.single(RenameNxRequest(oldKey, newKey))

  // --- RESTORE

  // --- SORT

  // --- TTL
  def ttlRequest(key: String): Source[TTLRequest, NotUsed] = Source.single(TTLRequest(key))

  // --- TYPE
  def typeRequest(key: String): Source[TypeRequest, NotUsed] = Source.single(TypeRequest(key))

  // --- WAIT

  // --- SCAN

}
