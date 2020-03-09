/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import java.util.Collections

import akka.annotation.InternalApi
import com.google.auth.oauth2.GoogleCredentials
import io.grpc.CallCredentials
import io.grpc.auth.MoreCallCredentials

/**
 * Internal API
 */
@InternalApi private[bigquery] object GrpcCredentials {

  def applicationDefault(): CallCredentials =
    MoreCallCredentials.from(
      GoogleCredentials.getApplicationDefault.createScoped(
        Collections.singletonList("https://www.googleapis.com/auth/bigquery.readonly")
      )
    )

}
