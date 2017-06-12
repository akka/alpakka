import sbt._, Keys._

object Dependencies {

  val AkkaVersion = sys.env.get("AKKA_SERIES") match {
    case Some("2.5") => "2.5.1"
    case _ => "2.4.18"
  }
  val AkkaHttpVersion = "10.0.6"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream"         % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion   % Test,
      "org.scalatest"     %% "scalatest"           % "3.0.1"       % Test, // ApacheV2
      "com.novocode"       % "junit-interface"     % "0.11"        % Test, // BSD-style
      "junit"              % "junit"               % "4.12"        % Test  // Eclipse Public License 1.0
    )
  )

  val Amqp = Seq(
    libraryDependencies ++= Seq(
      "com.rabbitmq" % "amqp-client" % "3.6.1" // APLv2
    )
  )

  val AwsLambda = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws"   % "aws-java-sdk-lambda"    % "1.11.105", // ApacheV2
      "org.mockito"   % "mockito-core"     % "2.7.17"    % Test  // MIT
    )
  )

  val AzureStorageQueue = Seq(
    libraryDependencies ++= Seq(
      "com.microsoft.azure" % "azure-storage" % "5.0.0" // ApacheV2
    )
  )

  val Cassandra = Seq(
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0" // ApacheV2
    )
  )

  val Csv = Seq()

  val DynamoDB = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws"     % "aws-java-sdk-dynamodb" % "1.11.106",          // ApacheV2
      "com.typesafe.akka" %% "akka-http"            % AkkaHttpVersion
    )
  )

  val File = Seq(
    libraryDependencies ++= Seq(
      "com.google.jimfs" % "jimfs" % "1.1"  % Test  // ApacheV2
    )
  )

  val Ftp = Seq(
    libraryDependencies ++= Seq(
      "commons-net"          % "commons-net"     % "3.5",            // ApacheV2
      "com.jcraft"           % "jsch"            % "0.1.54",         // BSD-style
      "org.apache.ftpserver" % "ftpserver-core"  % "1.0.6"  % Test, // ApacheV2
      "org.apache.sshd"      % "sshd-core"       % "1.3.0"  % Test, // ApacheV2
      "com.google.jimfs"     % "jimfs"           % "1.1"    % Test, // ApacheV2
      "org.slf4j"            % "slf4j-api"       % "1.7.21" % Test, // MIT
      "ch.qos.logback"       % "logback-classic" % "1.1.7"  % Test, // Eclipse Public License 1.0
      "ch.qos.logback"       % "logback-core"    % "1.1.7"  % Test  // Eclipse Public License 1.0
    )
  )

  val Geode = {
    val geodeVersion = "1.1.1"
    Seq(
      libraryDependencies ++= Seq("com.chuusai" %% "shapeless" % "2.3.2") ++
        Seq("geode-core","geode-cq")
          .map("org.apache.geode" % _ % geodeVersion exclude("org.slf4j", "slf4j-log4j12")) ++
        Seq("org.slf4j" % "log4j-over-slf4j" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
          "org.slf4j" % "slf4j-api" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
          "ch.qos.logback" % "logback-classic" % "1.1.7" % Test, // Eclipse Public License 1.0: http://logback.qos.ch/license.html
          "ch.qos.logback" % "logback-core" % "1.1.7" % Test // Eclipse Public License 1.0: http://logback.qos.ch/license.html
        )

    )
  }

  val GooglePubSub = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka"     %% "akka-http"     % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      "org.mockito"   % "mockito-core"     % "2.3.7"    % Test, // MIT
      "com.github.tomakehurst" % "wiremock"      % "2.5.1" % Test //ApacheV2
    )
  )

  val HBase = {
    val hbaseVersion = "1.2.4"
    val hadoopVersion = "2.5.1"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hbase" % "hbase-client" % hbaseVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hbase" % "hbase-common" % hbaseVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.slf4j" % "log4j-over-slf4j" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
        "org.slf4j" % "slf4j-api" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
        "ch.qos.logback" % "logback-classic" % "1.1.7" % Test, // Eclipse Public License 1.0: http://logback.qos.ch/license.html
        "ch.qos.logback" % "logback-core" % "1.1.7" % Test // Eclipse Public License 1.0: http://logback.qos.ch/license.html
      )
    )
  }

  val IronMq = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka"   %% "akka-http"                        % AkkaHttpVersion,
      "de.heikoseeberger"   %% "akka-http-circe"                  % "1.11.0"  // ApacheV2
    )
  )

  val Jms = Seq(
    libraryDependencies ++= Seq(
      "javax.jms" % "jms" % "1.1" % Provided, // CDDL + GPLv2
      "org.apache.activemq" % "activemq-broker" % "5.14.1" % Test, // ApacheV2
      "org.apache.activemq" % "activemq-client" % "5.14.1" % Test // ApacheV2
    ),
    // Having JBoss as a first resolver is a workaround for https://github.com/coursier/coursier/issues/200
    externalResolvers := ("jboss" at "https://repository.jboss.org/nexus/content/groups/public") +: externalResolvers.value
  )

  val KairosDB = Seq (
    libraryDependencies ++= Seq (
      "org.kairosdb"    % "client"         % "2.1.1",           // ApacheV2
      "org.mockito"     % "mockito-core"   % "2.3.7"    % Test  // MIT

    )
  )

  val Mqtt = Seq(
    libraryDependencies ++= Seq(
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0" // Eclipse Public License 1.0
    )
  )

  val S3 = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka"     %% "akka-http"     % AkkaHttpVersion,
      "com.typesafe.akka"     %% "akka-http-xml" % AkkaHttpVersion,
      // in-memory filesystem for file related tests
      "com.google.jimfs"       % "jimfs"         % "1.1"   % Test, // ApacheV2
      "com.github.tomakehurst" % "wiremock"      % "2.5.1" % Test //ApacheV2
    )
  )

  val Sns = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sns" % "1.11.95",        // ApacheV2
      "org.mockito"   % "mockito-core"     % "2.7.11"    % Test // MIT
    )
  )

  val Sqs = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws"     %  "aws-java-sdk-sqs"    % "1.11.109",             // ApacheV2
      "org.elasticmq"     %% "elasticmq-rest-sqs"  % "0.13.4"        % Test excludeAll(
        // elasticmq-rest-sqs depends on Akka 2.5, exclude it, so we can choose Akka version
        ExclusionRule(organization = "com.typesafe.akka") //
        ), // ApacheV2
      // elasticmq-rest-sqs depends on akka-slf4j which was excluded
      "com.typesafe.akka" %% "akka-slf4j"           % AkkaVersion    % Test, // ApacheV2
      // pull up akka-http version to the latest version for elasticmq-rest-sqs
      "com.typesafe.akka" %% "akka-http"           % AkkaHttpVersion % Test, // ApacheV2
      "org.mockito"       %  "mockito-core"        % "2.7.17"        % Test  // MIT
    )
  )

  val Sse = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"         % AkkaHttpVersion,
      "de.heikoseeberger" %% "akka-sse"          % "3.0.0", // ApacheV2
      "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % Test
    )
  )

  val Xml = Seq(
    libraryDependencies ++= Seq(
      "com.fasterxml" % "aalto-xml" % "1.0.0", // ApacheV2,
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0" // BSD-style
    )
  )
}