/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession

object BigQueryConfig {

  /**
   * Java API
   */
  def create(clientEmail: String,
             privateKey: String,
             projectId: String,
             dataset: String,
             actorSystem: ActorSystem): BigQueryConfig = {
    apply(clientEmail, privateKey, projectId, dataset)(actorSystem)
  }

  def apply(clientEmail: String, privateKey: String, projectId: String, dataset: String)(
      implicit actorSystem: ActorSystem
  ): BigQueryConfig = {
    val session = GoogleSession(clientEmail, privateKey, actorSystem)
    new BigQueryConfig(projectId, dataset, session)
  }

}

class BigQueryConfig(val projectId: String,
                     val dataset: String,
                     @InternalApi private[bigquery] val session: GoogleSession)
