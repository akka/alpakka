lazy val modules: Seq[ProjectReference] = Seq(
  amqp,
  avroparquet,
  awslambda,
  azureStorageQueue,
  cassandra,
  couchbase,
  csv,
  dynamodb,
  elasticsearch,
  files,
  ftp,
  geode,
  googleCloudPubSub,
  googleCloudPubSubGrpc,
  googleFcm,
  hbase,
  hdfs,
  ironmq,
  jms,
  jsonStreaming,
  kinesis,
  kudu,
  mongodb,
  mqtt,
  mqttStreaming,
  orientdb,
  reference,
  s3,
  springWeb,
  simpleCodecs,
  slick,
  sns,
  solr,
  sqs,
  sse,
  text,
  udp,
  unixdomainsocket,
  xml
)

lazy val alpakka = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .aggregate(modules: _*)
  .aggregate(`doc-examples`)
  .settings(
    onLoadMessage :=
      """
        |** Welcome to the sbt build definition for Alpakka! **
        |
        |Useful sbt tasks:
        |
        |  docs/previewSite - builds Paradox and Scaladoc documentation,
        |    starts a webserver and opens a new browser window
        |
        |  test - runs all the tests for all of the connectors.
        |    Make sure to run `docker-compose up` first.
        |
        |  mqtt/testOnly *.MqttSourceSpec - runs a single test
        |
        |  mimaReportBinaryIssues - checks whether this current API
        |    is binary compatible with the released version
      """.stripMargin,
    // unidoc combines sources and jars from all connectors and that
    // might include some incompatible ones. Depending on the
    // classpath order that might lead to scaladoc compilation errors.
    // Therefore some versions are exlcuded here.
    ScalaUnidoc / unidoc / fullClasspath := {
      (ScalaUnidoc / unidoc / fullClasspath).value
        .filterNot(_.data.getAbsolutePath.contains("protobuf-java-2.5.0.jar"))
        .filterNot(_.data.getAbsolutePath.contains("guava-26.0-android.jar"))
    },
    ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject -- inProjects(`doc-examples`),
    crossScalaVersions := List() // workaround for https://github.com/sbt/sbt/issues/3465
  )

lazy val amqp = alpakkaProject("amqp", "amqp", Dependencies.Amqp)

lazy val avroparquet =
  alpakkaProject("avroparquet", "avroparquet", Dependencies.AvroParquet, parallelExecution in Test := false)

lazy val awslambda = alpakkaProject("awslambda",
                                    "aws.lambda",
                                    Dependencies.AwsLambda,
                                    // For mockito https://github.com/akka/alpakka/issues/390
                                    parallelExecution in Test := false)

lazy val azureStorageQueue = alpakkaProject("azure-storage-queue", "azure.storagequeue", Dependencies.AzureStorageQueue)

lazy val cassandra = alpakkaProject("cassandra", "cassandra", Dependencies.Cassandra)

lazy val couchbase =
  alpakkaProject("couchbase", "couchbase", Dependencies.Couchbase, parallelExecution in Test := false)

lazy val csv = alpakkaProject("csv", "csv", Dependencies.Csv)

lazy val dynamodb = alpakkaProject("dynamodb", "aws.dynamodb", Dependencies.DynamoDB)

lazy val elasticsearch = alpakkaProject(
  "elasticsearch",
  "elasticsearch",
  Dependencies.Elasticsearch,
  // For elasticsearch-cluster-runner https://github.com/akka/alpakka/issues/479
  parallelExecution in Test := false
)

// The name 'file' is taken by `sbt.file`, hence 'files'
lazy val files = alpakkaProject("file", "file", Dependencies.File)

lazy val ftp = alpakkaProject(
  "ftp",
  "ftp",
  Dependencies.Ftp,
  parallelExecution in Test := false,
  fork in Test := true,
  // To avoid potential blocking in machines with low entropy (default is `/dev/random`)
  javaOptions in Test += "-Djava.security.egd=file:/dev/./urandom",
  crossScalaVersions -= Dependencies.Scala213
)

lazy val geode =
  alpakkaProject(
    "geode",
    "geode",
    Dependencies.Geode,
    fork in Test := true,
    parallelExecution in Test := false,
    unmanagedSourceDirectories in Compile ++= {
      val sourceDir = (sourceDirectory in Compile).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n >= 12 => Seq(sourceDir / "scala-2.12+")
        case _ => Seq.empty
      }
    }
  )

lazy val googleCloudPubSub = alpakkaProject(
  "google-cloud-pub-sub",
  "google.cloud.pubsub",
  Dependencies.GooglePubSub,
  fork in Test := true,
  envVars in Test := Map("PUBSUB_EMULATOR_HOST" -> "localhost:8538"),
  // For mockito https://github.com/akka/alpakka/issues/390
  parallelExecution in Test := false
)

lazy val googleCloudPubSubGrpc = alpakkaProject(
  "google-cloud-pub-sub-grpc",
  "google.cloud.pubsub.grpc",
  Dependencies.GooglePubSubGrpc,
  akkaGrpcCodeGeneratorSettings ~= { _.filterNot(_ == "flat_package") },
  akkaGrpcGeneratedSources := Seq(AkkaGrpc.Client),
  akkaGrpcGeneratedLanguages := Seq(AkkaGrpc.Scala, AkkaGrpc.Java),
  javaAgents += Dependencies.GooglePubSubGrpcAlpnAgent % "test",
  // for the ExampleApp in the tests
  connectInput in run := true,
  Compile / compile / scalacOptions += "-P:silencer:pathFilters=src_managed",
  crossScalaVersions --= Seq(Dependencies.Scala211, Dependencies.Scala213)
).enablePlugins(AkkaGrpcPlugin, JavaAgent)

lazy val googleFcm = alpakkaProject(
  "google-fcm",
  "google.firebase.fcm",
  Dependencies.GoogleFcm,
  fork in Test := true
)

lazy val hbase = alpakkaProject("hbase", "hbase", Dependencies.HBase, fork in Test := true)

lazy val hdfs = alpakkaProject("hdfs", "hdfs", Dependencies.Hdfs, parallelExecution in Test := false)

lazy val ironmq = alpakkaProject("ironmq",
                                 "ironmq",
                                 Dependencies.IronMq,
                                 fork in Test := true,
                                 crossScalaVersions -= Dependencies.Scala213)

lazy val jms = alpakkaProject("jms", "jms", Dependencies.Jms, parallelExecution in Test := false)

lazy val jsonStreaming = alpakkaProject("json-streaming", "json.streaming", Dependencies.JsonStreaming)

lazy val kinesis = alpakkaProject("kinesis",
                                  "aws.kinesis",
                                  Dependencies.Kinesis,
                                  // For mockito https://github.com/akka/alpakka/issues/390
                                  parallelExecution in Test := false)

lazy val kudu = alpakkaProject("kudu", "kudu", Dependencies.Kudu, fork in Test := false)

lazy val mongodb =
  alpakkaProject("mongodb", "mongodb", Dependencies.MongoDb, crossScalaVersions -= Dependencies.Scala213)

lazy val mqtt = alpakkaProject("mqtt", "mqtt", Dependencies.Mqtt)

lazy val mqttStreaming = alpakkaProject("mqtt-streaming", "mqttStreaming", Dependencies.MqttStreaming)
lazy val mqttStreamingBench = alpakkaProject("mqtt-streaming-bench", "mqttStreamingBench", publish / skip := true)
  .enablePlugins(JmhPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .dependsOn(mqtt, mqttStreaming)

lazy val orientdb = alpakkaProject("orientdb",
                                   "orientdb",
                                   Dependencies.OrientDB,
                                   fork in Test := true,
                                   parallelExecution in Test := false)

lazy val reference = alpakkaProject("reference", "reference", Dependencies.Reference, publish / skip := true)
  .disablePlugins(BintrayPlugin, MimaPlugin)

lazy val s3 = alpakkaProject("s3", "aws.s3", Dependencies.S3)

lazy val springWeb = alpakkaProject("spring-web", "spring.web", Dependencies.SpringWeb)

lazy val simpleCodecs = alpakkaProject("simple-codecs", "simplecodecs")

lazy val slick = alpakkaProject("slick", "slick", Dependencies.Slick, crossScalaVersions -= Dependencies.Scala213)

lazy val sns = alpakkaProject(
  "sns",
  "aws.sns",
  Dependencies.Sns,
  // For mockito https://github.com/akka/alpakka/issues/390
  parallelExecution in Test := false,
  crossScalaVersions -= Dependencies.Scala213
)

lazy val solr = alpakkaProject("solr", "solr", Dependencies.Solr, parallelExecution in Test := false)

lazy val sqs = alpakkaProject(
  "sqs",
  "aws.sqs",
  Dependencies.Sqs,
  // For mockito https://github.com/akka/alpakka/issues/390
  parallelExecution in Test := false,
  crossScalaVersions -= Dependencies.Scala213
)

lazy val sse = alpakkaProject("sse", "sse", Dependencies.Sse)

lazy val text = alpakkaProject("text", "text")

lazy val udp = alpakkaProject("udp", "udp")

lazy val unixdomainsocket = alpakkaProject(
  "unix-domain-socket",
  "unixdomainsocket",
  Dependencies.UnixDomainSocket
)

lazy val xml = alpakkaProject("xml", "xml", Dependencies.Xml)

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .settings(
    name := "Alpakka",
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    Preprocess / siteSubdirName := s"api/alpakka/${if (isSnapshot.value) "snapshot" else version.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
      ("\\.java\\.scala".r, _ => ".java")
    ),
    Paradox / siteSubdirName := s"docs/alpakka/${if (isSnapshot.value) "snapshot" else version.value}",
    Paradox / sourceDirectory := sourceDirectory.value / "main",
    Paradox / paradoxProperties ++= Map(
      "akka.version" -> Dependencies.AkkaVersion,
      "akka-http.version" -> Dependencies.AkkaHttpVersion,
      "couchbase.version" -> Dependencies.CouchbaseVersion,
      "hadoop.version" -> Dependencies.HadoopVersion,
      "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s",
      "extref.akka-http-docs.base_url" -> s"https://doc.akka.io/docs/akka-http/${Dependencies.AkkaHttpVersion}/%s",
      "extref.couchbase.base_url" -> s"https://docs.couchbase.com/java-sdk/${Dependencies.CouchbaseVersionForDocs}/%s",
      "extref.java-api.base_url" -> "https://docs.oracle.com/javase/8/docs/api/index.html?%s.html",
      "extref.geode.base_url" -> s"https://geode.apache.org/docs/guide/${Dependencies.GeodeVersionForDocs}/%s",
      "extref.javaee-api.base_url" -> "https://docs.oracle.com/javaee/7/api/index.html?%s.html",
      "extref.paho-api.base_url" -> "https://www.eclipse.org/paho/files/javadoc/index.html?%s.html",
      "extref.slick.base_url" -> s"https://slick.lightbend.com/doc/${Dependencies.SlickVersion}/%s",
      // Solr
      "extref.solr.base_url" -> s"https://lucene.apache.org/solr/guide/${Dependencies.SolrVersionForDocs}/%s",
      "javadoc.org.apache.solr.base_url" -> s"https://lucene.apache.org/solr/${Dependencies.SolrVersionForDocs}_0/solr-solrj/",
      // Java
      "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
      "javadoc.javax.jms.base_url" -> "https://docs.oracle.com/javaee/7/api/",
      "javadoc.com.couchbase.base_url" -> s"https://docs.couchbase.com/sdk-api/couchbase-java-client-${Dependencies.CouchbaseVersion}/",
      "javadoc.org.apache.kudu.base_url" -> s"https://kudu.apache.org/releases/${Dependencies.KuduVersion}/apidocs/",
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersion}/",
      "javadoc.akka.http.base_url" -> s"https://doc.akka.io/japi/akka-http/${Dependencies.AkkaHttpVersion}/",
      "javadoc.org.apache.hadoop.base_url" -> s"https://hadoop.apache.org/docs/r${Dependencies.HadoopVersion}/api/",
      "javadoc.com.amazonaws.base_url" -> "https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/",
      // Eclipse Paho client for MQTT
      "javadoc.org.eclipse.paho.client.mqttv3.base_url" -> "https://www.eclipse.org/paho/files/javadoc/",
      "javadoc.org.bson.codecs.configuration.base_url" -> "https://mongodb.github.io/mongo-java-driver/3.7/javadoc/",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
      "scaladoc.akka.http.base_url" -> s"https://doc.akka.io/api/akka-http/${Dependencies.AkkaHttpVersion}/",
      "scaladoc.akka.stream.alpakka.base_url" -> {
        val docsHost = sys.env
          .get("CI")
          .map(_ => "https://doc.akka.io")
          .getOrElse(s"http://localhost:${(previewSite / previewFixedPort).value}")
        s"$docsHost/api/alpakka/${if (isSnapshot.value) "snapshot" else version.value}/"
      }
    ),
    Paradox / paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )

lazy val `doc-examples` = project
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin, SitePlugin)
  .dependsOn(
    modules.map(p => classpathDependency(p)): _*
  )
  .settings(
    name := s"akka-stream-alpakka-doc-examples",
    publish / skip := true,
    whitesourceIgnore := true,
    // Google Cloud Pub/Sub gRPC is not available for Scala 2.11
    crossScalaVersions -= Dependencies.Scala211,
    // More projects are not available for Scala 2.13
    crossScalaVersions -= Dependencies.Scala213,
    Dependencies.`Doc-examples`
  )

def alpakkaProject(projectId: String, moduleName: String, additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = projectId, base = file(projectId))
    .enablePlugins(AutomateHeaderPlugin)
    .disablePlugins(SitePlugin)
    .settings(
      name := s"akka-stream-alpakka-$projectId",
      AutomaticModuleName.settings(s"akka.stream.alpakka.$moduleName"),
      mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
    )
    .settings(additionalSettings: _*)

Global / onLoad := (Global / onLoad).value.andThen { s =>
  val v = version.value
  if (dynverGitDescribeOutput.value.hasNoTags)
    throw new MessageOnlyException(
      s"Failed to derive version from git tags. Maybe run `git fetch --unshallow`? Derived version: $v"
    )
  s
}
