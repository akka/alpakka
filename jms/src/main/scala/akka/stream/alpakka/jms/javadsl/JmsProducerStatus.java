/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Source;

public interface JmsProducerStatus {

    /**
     * source that provides connector status change information.
     * Only the most recent connector state is buffered if the source is not consumed.
     */
    Source<JmsConnectorState, NotUsed> connectorState();
}
