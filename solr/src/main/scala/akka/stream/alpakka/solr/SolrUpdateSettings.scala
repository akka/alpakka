/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import scala.concurrent.duration.{FiniteDuration, _}

//#solr-update-settings
final case class SolrUpdateSettings(
    commitWithin: Int = -1
) {
  def withCommitWithin(commitWithin: Int): SolrUpdateSettings =
    copy(commitWithin = commitWithin)
}
//#solr-update-settings

object SolrUpdateSettings {

  /**
   * Java API
   */
  def create(): SolrUpdateSettings = SolrUpdateSettings()
}
