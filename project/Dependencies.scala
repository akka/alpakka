import sbt._, Keys._

object Dependencies {

  val AkkaVersion = sys.env.get("AKKA_SERIES") match {
    case Some("2.4") => sys.error("Akka 2.4 is not supported anymore")
    case _ => "2.5.8"
  }

  val AwsSdkVersion = "1.11.226"
  val AkkaHttpVersion = "10.0.11"

  val Common = Seq(
    // These libraries are added to all modules via the `Common` AutoPlugin
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Test, // Eclipse Public License 1.0
      "org.scalatest" %% "scalatest" % "3.0.4" % Test, // ApacheV2
      "com.novocode" % "junit-interface" % "0.11" % Test, // BSD-style
      "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    )
  )

  val Amqp = Seq(
    libraryDependencies ++= Seq(
      "com.rabbitmq" % "amqp-client" % "3.6.1" // APLv2
    )
  )

  val AwsLambda = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-lambda" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.7.17" % Test // MIT
    )
  )

  val AzureStorageQueue = Seq(
    libraryDependencies ++= Seq(
      "com.microsoft.azure" % "azure-storage" % "5.0.0" // ApacheV2
    )
  )

  val Cassandra = Seq(
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0" // ApacheV2
    )
  )

  val Csv = Seq(
    libraryDependencies ++= Seq()
  )

  val DynamoDB = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-dynamodb" % AwsSdkVersion, // ApacheV2
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
    )
  )

  val Elasticsearch = Seq(
    libraryDependencies ++= Seq(
      "org.elasticsearch.client" % "rest" % "5.5.3", // ApacheV2
      "io.spray" %% "spray-json" % "1.3.3", // ApacheV2
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1", // ApacheV2
      "org.codelibs" % "elasticsearch-cluster-runner" % "5.6.0.0" % Test // ApacheV2
    )
  )

  val File = Seq(
    libraryDependencies ++= Seq(
      "com.google.jimfs" % "jimfs" % "1.1" % Test // ApacheV2
    )
  )

  val Ftp = Seq(
    libraryDependencies ++= Seq(
      "commons-net" % "commons-net" % "3.6", // ApacheV2
      "com.hierynomus" % "sshj" % "0.21.1", // ApacheV2
      "org.apache.ftpserver" % "ftpserver-core" % "1.1.1" % Test, // ApacheV2
      "org.apache.sshd" % "sshd-core" % "1.6.0" % Test, // ApacheV2
      "net.i2p.crypto" % "eddsa" % "0.2.0" % Test, // CC0 1.0 Universal
      "com.google.jimfs" % "jimfs" % "1.1" % Test // ApacheV2
    )
  )

  val Geode = {
    val geodeVersion = "1.3.0"
    val slf4jVersion = "1.7.25"
    Seq(
      libraryDependencies ++=
      Seq("geode-core", "geode-cq")
        .map("org.apache.geode" % _ % geodeVersion exclude ("org.slf4j", "slf4j-log4j12")) ++
      Seq(
        "com.chuusai" %% "shapeless" % "2.3.2",
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test // MIT like: http://www.slf4j.org/license.html
      )
    )
  }

  val GooglePubSub = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      "org.mockito" % "mockito-core" % "2.3.7" % Test, // MIT
      "com.github.tomakehurst" % "wiremock" % "2.5.1" % Test // ApacheV2
    )
  )

  val HBase = {
    val hbaseVersion = "1.2.4"
    val hadoopVersion = "2.5.1"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hbase" % "hbase-client" % hbaseVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hbase" % "hbase-common" % hbaseVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.slf4j" % "log4j-over-slf4j" % "1.7.21" % Test // MIT like: http://www.slf4j.org/license.html
      )
    )
  }

  val IronMq = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "de.heikoseeberger" %% "akka-http-circe" % "1.11.0" // ApacheV2
    )
  )

  val Jms = Seq(
    libraryDependencies ++= Seq(
      "javax.jms" % "jms" % "1.1" % Provided, // CDDL + GPLv2
      "org.apache.activemq" % "activemq-broker" % "5.14.1" % Test, // ApacheV2
      "org.apache.activemq" % "activemq-client" % "5.14.1" % Test // ApacheV2
    ),
    resolvers += ("jboss" at "https://repository.jboss.org/nexus/content/groups/public")
  )

  val Kinesis = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-kinesis" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.7.11" % Test // MIT
    )
  )
  val MongoDb = Seq(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0" // ApacheV2
    )
  )

  val Mqtt = Seq(
    libraryDependencies ++= Seq(
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.0" // Eclipse Public License 1.0
    )
  )

  val S3 = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml" % AkkaHttpVersion,
      "com.amazonaws" % "aws-java-sdk-core" % AwsSdkVersion, // ApacheV2
      // in-memory filesystem for file related tests
      "com.google.jimfs" % "jimfs" % "1.1" % Test, // ApacheV2
      "com.github.tomakehurst" % "wiremock" % "2.5.1" % Test // ApacheV2
    )
  )

  val SpringWeb = {
    val SpringVersion = "5.0.0.RELEASE"
    val SpringBootVersion = "1.5.7.RELEASE"
    Seq(
      libraryDependencies ++= Seq(
        "org.springframework" % "spring-core" % SpringVersion,
        "org.springframework" % "spring-context" % SpringVersion,
        "org.springframework" % "spring-webflux" % SpringVersion,
        "org.springframework" % "spring-webmvc" % SpringVersion,
        "org.springframework.boot" % "spring-boot-autoconfigure" % SpringBootVersion, // TODO should this be provided?

        // for examples
        "org.springframework.boot" % "spring-boot-starter-web" % SpringBootVersion % Test
      )
    )
  }

  val Slick = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % "3.2.1", // BSD 2-clause "Simplified" License
      "com.typesafe.slick" %% "slick-hikaricp" % "3.2.1", // BSD 2-clause "Simplified" License
      "com.h2database" % "h2" % "1.4.196" % Test // Eclipse Public License 1.0
    )
  )

  val Sns = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sns" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.12.0" % Test // MIT
    )
  )

  val Sqs = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sqs" % AwsSdkVersion, // ApacheV2
      "org.elasticmq" %% "elasticmq-rest-sqs" % "0.13.8" % Test excludeAll (
        // elasticmq-rest-sqs depends on Akka 2.5, exclude it, so we can choose Akka version
        ExclusionRule(organization = "com.typesafe.akka") //
      ), // ApacheV2
      // pull up akka-http version to the latest version for elasticmq-rest-sqs
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion % Test, // ApacheV2
      "org.mockito" % "mockito-core" % "2.12.0" % Test // MIT
    )
  )

  val Sse = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % Test
    )
  )

  val UnixDomainSocket = Seq(
    libraryDependencies ++= Seq(
      "com.github.jnr" % "jnr-unixsocket" % "0.18" // BSD/ApacheV2/CPL/MIT as per https://github.com/akka/alpakka/issues/620#issuecomment-348727265
    )
  )

  val Xml = Seq(
    libraryDependencies ++= Seq(
      "com.fasterxml" % "aalto-xml" % "1.0.0", // ApacheV2,
      "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0" // BSD-style
    )
  )

}
