/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner

object ElasticSearchEmbedded {
  private val runner = new ElasticsearchClusterRunner()

  def startElasticInstance() = {
    runner.build(
      ElasticsearchClusterRunner
        .newConfigs()
        .baseHttpPort(9200)
        .baseTransportPort(9300)
        .numOfNode(1)
        .disableESLogger()
    )
    runner.ensureYellow()
    runner
  }
}
