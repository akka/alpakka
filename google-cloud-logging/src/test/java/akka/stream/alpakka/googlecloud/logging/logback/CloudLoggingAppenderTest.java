/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.logback;

import akka.actor.ActorSystem;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

public class CloudLoggingAppenderTest {

  static ActorSystem system = null;

  void logEvents() {
    // #logback-setup
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

    CloudLoggingAppender appender = new CloudLoggingAppender();
    appender.setContext(context);
    appender.setActorSystem(system);
    appender.setName("cloud");
    appender.addEnhancer(MDCEventEnhancer.class.getName());
    appender.addEnhancer(TestEnhancer.class.getName());
    appender.start();

    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.addAppender(appender);
    // #logback-setup
  }
}
