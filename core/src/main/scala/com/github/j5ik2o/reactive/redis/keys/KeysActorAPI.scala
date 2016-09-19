package com.github.j5ik2o.reactive.redis.keys

import akka.actor.Actor
import com.github.j5ik2o.reactive.redis.BaseActorAPI
import akka.pattern.{ ask, pipe }

trait KeysActorAPI extends BaseActorAPI with KeysStreamAPI {
  this: Actor =>
  import KeysProtocol._

  import context.dispatcher

  val handleKeys: Receive = {
    // --- DEL
    case DelRequest(keys) =>
      del(keys).map { v =>
        DelSucceeded(v)
      }.recover { case ex: Exception =>
        DelFailure(ex)
      }.pipeTo(sender())

    // --- DUMP

    // --- EXISTS
    case ExistsRequest(key) =>
      exists(key).map { v =>
        ExistsSucceeded(v)
      }.recover { case ex: Exception =>
        ExistsFailure(ex)
      }.pipeTo(sender())


    // --- EXPIRE
    case ExpireRequest(key, timeout) =>
      expire(key, timeout).map { v =>
        ExpireSucceeded(v)
      }.recover { case ex: Exception =>
        ExpireFailure(ex)
      }.pipeTo(sender())
    // --- EXPIREAT
    case ExpireAtRequest(key, unixTimeout) =>
      expireAt(key, unixTimeout).map { v =>
        ExpireAtSucceeded(v)
      }.recover { case ex: Exception =>
        ExpireAtFailure(ex)
      }.pipeTo(sender())


    // --- KEYS
    case KeysRequest(keyPattern) =>
      run(keys(keyPattern)).pipeTo(sender())

    // --- MIGRATE

    // --- MOVE
    case MoveRequest(key, index) =>
      move(key, index).map { v =>
        MoveSucceeded
      }.recover { case ex: Exception =>
        MoveFailure(ex)
      }.pipeTo(sender())

    // --- OBJECT

    // --- PERSIST
    case PersistRequest(key) =>
      persist(key).map { v =>
        PersistSucceeded(v)
      }.recover { case ex: Exception =>
        PersistFailure(ex)
      }.pipeTo(sender())


    // --- PEXPIRE

    // --- PEXPIREAT

    // --- PTTL

    // --- RANDOMKEY
    case RandomKeyRequest =>
      randomKey.map { v =>
        RandomKeySucceeded
      }.recover { case ex: Exception =>
        RandomKeyFailure(ex)
      }.pipeTo(sender())

    // --- RENAME
    case RenameRequest(oldKey, newKey) =>
      rename(oldKey, newKey).map { _ =>
        RenameSucceeded
      }.recover { case ex: Exception =>
        RenameFailure(ex)
      }.pipeTo(sender())
    // --- RENAMENX
    case RenameNxRequest(oldKey, newKey) =>
      renameNx(oldKey, newKey).map { v =>
        RenameNxSucceeded(v)
      }.recover { case ex: Exception =>
        RenameNxFailure(ex)
      }.pipeTo(sender())


    // --- RESTORE

    // --- SCAN

    // --- SORT

    // --- TTL


    case TTLRequest(key) =>
      ttl(key).map { v =>
        TTLSucceeded(v)
      }.recover { case ex: Exception =>
        TTLFailure(ex)
      }.pipeTo(sender())

    // --- TYPE
    case TypeRequest(key) =>
      `type`(key).map { v =>
        TypeSucceeded(v)
      }.recover { case ex: Exception =>
        TypeFailure(ex)
      }.pipeTo(sender())

    // --- WAIT
  }
}
