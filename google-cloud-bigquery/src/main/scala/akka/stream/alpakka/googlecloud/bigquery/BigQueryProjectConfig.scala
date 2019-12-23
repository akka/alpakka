/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.impl.{GoogleSession, GoogleTokenApi}

object BigQueryProjectConfig {

  def apply(clientEmail: String,
            privateKey: String,
            projectId: String,
            dataset: String,
            actorSystem: ActorSystem): BigQueryProjectConfig = {
    val session = GoogleSession(clientEmail, privateKey, actorSystem)
    new BigQueryProjectConfig(projectId, dataset, session)
  }
}

class BigQueryProjectConfig(val projectId: String, val dataset: String, val session: GoogleSession)
