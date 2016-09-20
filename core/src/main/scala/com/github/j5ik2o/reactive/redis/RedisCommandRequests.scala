package com.github.j5ik2o.reactive.redis

import com.github.j5ik2o.reactive.redis.connection.ConnectionStreamAPI
import com.github.j5ik2o.reactive.redis.keys.KeysStreamAPI
import com.github.j5ik2o.reactive.redis.server.ServerStreamAPI
import com.github.j5ik2o.reactive.redis.transactions.TransactionsStreamAPI

trait RedisCommandRequests
  extends ConnectionStreamAPI
    with ServerStreamAPI
    with KeysStreamAPI
    with StringStreamAPI
    with TransactionsStreamAPI {

}

object RedisCommandRequests extends RedisCommandRequests

