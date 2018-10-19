/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

public enum JmsConnectorState {

    Disconnected, Connecting, Connected, Completing, Completed, Failing, Failed

}
