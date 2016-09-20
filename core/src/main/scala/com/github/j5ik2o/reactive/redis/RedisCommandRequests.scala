package com.github.j5ik2o.reactive.redis

import com.github.j5ik2o.reactive.redis.connection.ConnectionCommandRequests
import com.github.j5ik2o.reactive.redis.keys.KeysCommandRequests
import com.github.j5ik2o.reactive.redis.server.ServerCommandRequests
import com.github.j5ik2o.reactive.redis.strings.StringCommandRequests
import com.github.j5ik2o.reactive.redis.transactions.TransactionsCommandRequests

trait RedisCommandRequests
    extends ConnectionCommandRequests
    with ServerCommandRequests
    with KeysCommandRequests
    with TransactionsCommandRequests
    with StringCommandRequests {

}

object RedisCommandRequests extends RedisCommandRequests

