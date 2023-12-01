/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.javadsl;

public enum JmsConnectorState {

    Disconnected, Connecting, Connected, Completing, Completed, Failing, Failed

}
