package com.github.j5ik2o.reactive.redis.keys

import akka.actor.Actor
import com.github.j5ik2o.reactive.redis.BaseActorAPI
import akka.pattern.{ ask, pipe }

trait KeysActorAPI extends BaseActorAPI with KeysCommandRequests {
  this: Actor =>
  import KeysProtocol._

  import context.dispatcher

  val handleKeys: Receive = {
    // --- DEL
    case DelRequest(keys)                  =>

    // --- DUMP

    // --- EXISTS
    case ExistsRequest(key)                =>

    // --- EXPIRE
    case ExpireRequest(key, timeout)       =>

    // --- EXPIREAT
    case ExpireAtRequest(key, unixTimeout) =>

    // --- KEYS
    case KeysRequest(keyPattern)           =>
    //run(keys(keyPattern)).pipeTo(sender())

    // --- MIGRATE

    // --- MOVE
    case MoveRequest(key, index)           =>

    // --- OBJECT

    // --- PERSIST
    case PersistRequest(key)               =>

    // --- PEXPIRE

    // --- PEXPIREAT

    // --- PTTL

    // --- RANDOMKEY
    case RandomKeyRequest                  =>

    // --- RENAME
    case RenameRequest(oldKey, newKey)     =>

    // --- RENAMENX
    case RenameNxRequest(oldKey, newKey)   =>

    // --- RESTORE

    // --- SCAN

    // --- SORT

    // --- TTL
    case TTLRequest(key)                   =>

    // --- TYPE
    case TypeRequest(key)                  =>

    // --- WAIT
  }
}
