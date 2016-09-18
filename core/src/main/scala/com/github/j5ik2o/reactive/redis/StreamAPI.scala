package com.github.j5ik2o.reactive.redis

import com.github.j5ik2o.reactive.redis.connection.ConnectionStreamAPI
import com.github.j5ik2o.reactive.redis.keys.KeysStreamAPI
import com.github.j5ik2o.reactive.redis.server.ServerStreamAPI

trait StreamAPI
  extends ConnectionStreamAPI
    with ServerStreamAPI
    with KeysStreamAPI
    with StringStreamAPI {

}
