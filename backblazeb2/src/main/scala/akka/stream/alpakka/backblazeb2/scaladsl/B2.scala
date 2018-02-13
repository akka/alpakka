/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol.{B2AccountCredentials, BucketId}

/**
 * Factory class for B2 authorizer and client. Authorizers are reused between clients.
 */
class B2(
    accountCredentials: B2AccountCredentials,
    hostAndPort: String = B2API.DefaultHostAndPort
)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) {
  val api = new B2API(hostAndPort)

  def createAuthorizer(eager: Boolean = false): B2AccountAuthorizer =
    new B2AccountAuthorizer(api, accountCredentials, eager)
  def createClient(authorizer: B2AccountAuthorizer, bucketId: BucketId): B2Client =
    new B2Client(api, authorizer, bucketId)
}
