/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference

@InternalApi
private[paginated] final case class PagingInfo(jobInfo: Option[JobInfo], pageToken: Option[String])

@InternalApi
private[paginated] final case class JobInfo(jobReference: JobReference, jobComplete: Boolean)
