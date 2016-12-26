/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka

import com.amazonaws.services.sqs.model.Message

package object sqs {

  type MessageActionPair = (Message, MessageAction)
}
