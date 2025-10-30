/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.jakartajms.javadsl;

import akka.NotUsed;
import akka.stream.KillSwitch;
import akka.stream.javadsl.Source;

public interface JmsConsumerControl extends KillSwitch {

    /**
     * source that provides connector status change information.
     * Only the most recent connector state is buffered if the source is not consumed.
     */
    Source<JmsConnectorState, NotUsed> connectorState();
}
