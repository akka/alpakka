import sbt._
import Keys._

object Dependencies {

  val ScalaVersions = Seq("2.12.6", "2.11.12")

  val AkkaVersion = sys.env.get("AKKA_SERIES") match {
    case Some("2.4") => sys.error("Akka 2.4 is not supported anymore")
    case _ => "2.5.13"
  }

  val AwsSdkVersion = "1.11.371"
  val AkkaHttpVersion = "10.1.3"

  val Common = Seq(
    // These libraries are added to all modules via the `Common` AutoPlugin
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Test, // Eclipse Public License 1.0
      "org.scalatest" %% "scalatest" % "3.0.5" % Test, // ApacheV2
      "com.novocode" % "junit-interface" % "0.11" % Test, // BSD-style
      "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    )
  )

  val Amqp = Seq(
    libraryDependencies ++= Seq(
      "com.rabbitmq" % "amqp-client" % "5.3.0" // APLv2
    )
  )

  val AwsLambda = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-lambda" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.19.1" % Test // MIT
    )
  )

  val AzureStorageQueue = Seq(
    libraryDependencies ++= Seq(
      "com.microsoft.azure" % "azure-storage" % "8.0.0" // ApacheV2
    )
  )

  val Cassandra = Seq(
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1" // ApacheV2
    )
  )

  val Csv = Seq(
    libraryDependencies ++= Seq()
  )

  val `Doc-examples` = Seq(
    libraryDependencies ++= Seq(
      // https://mina.apache.org/ftpserver-project/downloads.html
      "org.apache.ftpserver" % "ftpserver-core" % "1.1.1", // ApacheV2
      "com.google.jimfs" % "jimfs" % "1.1", // ApacheV2
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http-xml" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      // https://github.com/akka/alpakka-kafka/releases
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
      // https://github.com/manub/scalatest-embedded-kafka/tags
      "net.manub" %% "scalatest-embedded-kafka" % "1.1.0", // MIT
      // https://github.com/javaee/javax.jms
      "javax.jms" % "jms" % "1.1", // CDDL Version 1.1
      // http://activemq.apache.org/download.html
      "org.apache.activemq" % "activemq-all" % "5.15.4" exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2
      "com.h2database" % "h2" % "1.4.197", // Eclipse Public License 1.0
      "org.elasticsearch.client" % "elasticsearch-rest-client" % "6.3.1", // ApacheV2
      "org.codelibs" % "elasticsearch-cluster-runner" % "6.3.1.0", // ApacheV2
      "io.netty" % "netty-all" % "4.1.27.Final", // ApacheV2
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.9.6",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.9.6",
      "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
      "org.slf4j" % "jcl-over-slf4j" % "1.7.25",
      "ch.qos.logback" % "logback-classic" % "1.2.3" // Eclipse Public License 1.0
    )
  )

  val DynamoDB = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-dynamodb" % AwsSdkVersion, // ApacheV2
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
    )
  )

  val Elasticsearch = Seq(
    libraryDependencies ++= Seq(
      "org.elasticsearch.client" % "elasticsearch-rest-client" % "6.3.1", // ApacheV2
      "io.spray" %% "spray-json" % "1.3.4", // ApacheV2
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6", // ApacheV2
      "org.codelibs" % "elasticsearch-cluster-runner" % "6.3.1.0" % Test // ApacheV2
    )
  )

  val File = Seq(
    libraryDependencies ++= Seq(
      "com.google.jimfs" % "jimfs" % "1.1" % Test // ApacheV2
    )
  )

  val AvroParquet = Seq(
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-avro" % "1.10.0", //Apache2
      "org.apache.hadoop" % "hadoop-client" % "3.1.0" % Test, //Apache2
      "org.apache.hadoop" % "hadoop-common" % "2.2.0" % Test, //Apache2
      "org.specs2" %% "specs2-core" % "4.3.2" % Test, //MIT like: https://github.com/etorreborre/specs2/blob/master/LICENSE.txt
      "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    )
  )

  val Ftp = Seq(
    libraryDependencies ++= Seq(
      "commons-net" % "commons-net" % "3.6", // ApacheV2
      "com.hierynomus" % "sshj" % "0.24.0", // ApacheV2
      "org.apache.ftpserver" % "ftpserver-core" % "1.1.1" % Test, // ApacheV2
      "org.apache.sshd" % "sshd-core" % "1.7.0" % Test, // ApacheV2
      "net.i2p.crypto" % "eddsa" % "0.3.0" % Test, // CC0 1.0 Universal
      "com.google.jimfs" % "jimfs" % "1.1" % Test // ApacheV2
    )
  )

  val Geode = {
    val geodeVersion = "1.6.0"
    val slf4jVersion = "1.7.25"
    Seq(
      libraryDependencies ++=
        Seq("geode-core", "geode-cq")
          .map("org.apache.geode" % _ % geodeVersion exclude ("org.slf4j", "slf4j-log4j12")) ++
        Seq(
          "com.chuusai" %% "shapeless" % "2.3.3",
          "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test // MIT like: http://www.slf4j.org/license.html
        )
    )
  }

  val GooglePubSub = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      "org.mockito" % "mockito-core" % "2.19.1" % Test, // MIT
      "com.github.tomakehurst" % "wiremock" % "2.18.0" % Test // ApacheV2
    )
  )

  val GoogleFcm = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
      "com.pauldijou" %% "jwt-core" % "0.16.0", //ApacheV2
      "org.mockito" % "mockito-core" % "2.19.1" % Test // MIT
    )
  )

  val HBase = {
    val hbaseVersion = "1.2.6.1"
    val hadoopVersion = "2.5.2"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hbase" % "hbase-client" % hbaseVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hbase" % "hbase-common" % hbaseVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % Test // MIT like: http://www.slf4j.org/license.html
      )
    )
  }

  val HadoopVersion = "3.1.0"
  val Hdfs = {
    val hadoopVersion = HadoopVersion
    val catsVersion = "1.1.0"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion, // ApacheV2
        "org.typelevel" %% "cats-core" % catsVersion, // MIT,
        //Test
        "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test classifier "tests", // ApacheV2
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test classifier "tests", // ApacheV2
        "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test // ApacheV2
      )
    )
  }

  val IronMq = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "de.heikoseeberger" %% "akka-http-circe" % "1.21.0" // ApacheV2
    )
  )

  val Jms = Seq(
    libraryDependencies ++= Seq(
      "javax.jms" % "jms" % "1.1" % Provided, // CDDL + GPLv2
      "org.apache.activemq" % "activemq-broker" % "5.15.4" % Test, // ApacheV2
      "org.apache.activemq" % "activemq-client" % "5.15.4" % Test, // ApacheV2
      "org.mockito" % "mockito-core" % "2.21.0" % Test // MIT
    ),
    resolvers += ("jboss" at "https://repository.jboss.org/nexus/content/groups/public")
  )

  val JsonStreaming = Seq(
    libraryDependencies ++= Seq(
      "com.github.jsurfer" % "jsurfer" % "1.4.3", // MIT,
      "com.github.jsurfer" % "jsurfer-jackson" % "1.4.3" // MIT
    )
  )

  val Kinesis = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-kinesis" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.19.1" % Test // MIT
    )
  )

  val Kudu = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kudu" % "kudu-client-tools" % "1.7.1", // ApacheV2
      "org.apache.kudu" % "kudu-client" % "1.7.1" % Test // ApacheV2
    )
  )

  val MongoDb = Seq(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.0" // ApacheV2
    )
  )

  val Mqtt = Seq(
    libraryDependencies ++= Seq(
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.0" // Eclipse Public License 1.0
    )
  )

  val OrientDB = Seq(
    libraryDependencies ++= Seq(
      "com.orientechnologies" % "orientdb-graphdb" % "3.0.4", // ApacheV2
      "com.orientechnologies" % "orientdb-object" % "3.0.4" // ApacheV2
    )
  )

  val PostgreSqlCdc = Seq(
    libraryDependencies ++= Seq("com.lihaoyi" %% "fastparse" % "1.0.0", // The MIT License (MIT)
                                "org.postgresql" % "postgresql" % "42.2.1" % Test) // BSD 2-clause "Simplified" License
  )

  val Reference = Seq(
    // connector specific library dependencies and resolver settings
    libraryDependencies ++= Seq(
      )
  )

  val S3 = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml" % AkkaHttpVersion,
      "com.amazonaws" % "aws-java-sdk-core" % AwsSdkVersion, // ApacheV2
      // in-memory filesystem for file related tests
      "com.google.jimfs" % "jimfs" % "1.1" % Test, // ApacheV2
      "com.github.tomakehurst" % "wiremock" % "2.18.0" % Test // ApacheV2
    )
  )

  val SpringWeb = {
    val SpringVersion = "5.0.7.RELEASE"
    val SpringBootVersion = "1.5.14.RELEASE"
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

  val SlickVersion = "3.2.3"
  val Slick = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % SlickVersion, // BSD 2-clause "Simplified" License
      "com.typesafe.slick" %% "slick-hikaricp" % SlickVersion, // BSD 2-clause "Simplified" License
      "com.h2database" % "h2" % "1.4.196" % Test // Eclipse Public License 1.0
    )
  )

  val Sns = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sns" % AwsSdkVersion, // ApacheV2
      "org.mockito" % "mockito-core" % "2.19.1" % Test // MIT
    )
  )

  val Solr = {
    val solrjVersion = "7.4.0"
    val slf4jVersion = "1.7.25"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.solr" % "solr-solrj" % solrjVersion, // ApacheV2
        //Test
        "org.apache.solr" % "solr-test-framework" % solrjVersion % Test, // ApacheV2
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion % Test // MIT like: http://www.slf4j.org/license.html
      ),
      resolvers += ("restlet" at "https://maven.restlet.com")
    )
  }

  val Sqs = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sqs" % AwsSdkVersion, // ApacheV2
      "org.elasticmq" %% "elasticmq-rest-sqs" % "0.14.1" % Test excludeAll (
        // elasticmq-rest-sqs depends on Akka 2.5, exclude it, so we can choose Akka version
        ExclusionRule(organization = "com.typesafe.akka") //
      ), // ApacheV2
      // pull up akka-http version to the latest version for elasticmq-rest-sqs
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion % Test, // ApacheV2
      "org.mockito" % "mockito-core" % "2.19.1" % Test // MIT
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
      "com.github.jnr" % "jnr-unixsocket" % "0.19" // BSD/ApacheV2/CPL/MIT as per https://github.com/akka/alpakka/issues/620#issuecomment-348727265
    )
  )

  val Xml = Seq(
    libraryDependencies ++= Seq(
      "com.fasterxml" % "aalto-xml" % "1.1.0" // ApacheV2
    )
  )

}
