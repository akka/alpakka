/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

case class FcmFlowConfig(clientEmail: String,
                         privateKey: String,
                         projectid: String,
                         isTest: Boolean = false,
                         maxConcurentConnections: Int = 100)
