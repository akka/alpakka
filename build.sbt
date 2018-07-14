lazy val modules: Seq[ProjectReference] = Seq(
  amqp,
  avroparquet,
  awslambda,
  azureStorageQueue,
  cassandra,
  csv,
  dynamodb,
  elasticsearch,
  files,
  ftp,
  geode,
  googleCloudPubSub,
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
  orientdb,
  postgresqlcdc,
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
  .enablePlugins(PublishUnidoc)
  .disablePlugins(MimaPlugin)
  .aggregate(modules: _*)
  .aggregate(`doc-examples`)
  .settings(
    onLoadMessage :=
      """
        |** Welcome to the sbt build definition for Alpakka! **
        |
        |Useful sbt tasks:
        |
        |  docs/Local/paradox - builds documentation with locally
        |    linked Scala API docs, which can be found at
        |    docs/target/paradox/site/local
        |
        |  test - runs all the tests for all of the connectors.
        |   Make sure to run `docker-compose up` first.
        |
        |  mqtt/testOnly *.MqttSourceSpec - runs a single test
        |
        |  mimaReportBinaryIssues - checks whether this current API
        |    is binary compatible with the released version
      """.stripMargin,
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% name.value % _).toSet
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
  javaOptions in Test += "-Djava.security.egd=file:/dev/./urandom"
)

lazy val geode =
  alpakkaProject("geode", "geode", Dependencies.Geode, fork in Test := true, parallelExecution in Test := false)

lazy val googleCloudPubSub = alpakkaProject(
  "google-cloud-pub-sub",
  "google.cloud.pubsub",
  Dependencies.GooglePubSub,
  fork in Test := true,
  envVars in Test := Map("PUBSUB_EMULATOR_HOST" -> "localhost:8538"),
  // For mockito https://github.com/akka/alpakka/issues/390
  parallelExecution in Test := false
)

lazy val googleFcm = alpakkaProject(
  "google-fcm",
  "google.firebase.fcm",
  Dependencies.GoogleFcm,
  fork in Test := true
)

lazy val hbase = alpakkaProject("hbase", "hbase", Dependencies.HBase, fork in Test := true)

lazy val hdfs = alpakkaProject("hdfs", "hdfs", Dependencies.Hdfs, parallelExecution in Test := false)

lazy val ironmq = alpakkaProject("ironmq", "ironmq", Dependencies.IronMq)

lazy val jms = alpakkaProject("jms", "jms", Dependencies.Jms, parallelExecution in Test := false)

lazy val jsonStreaming = alpakkaProject("json-streaming", "json.streaming", Dependencies.JsonStreaming)

lazy val kinesis = alpakkaProject("kinesis",
                                  "aws.kinesis",
                                  Dependencies.Kinesis,
                                  // For mockito https://github.com/akka/alpakka/issues/390
                                  parallelExecution in Test := false)

lazy val kudu = alpakkaProject("kudu", "kudu", Dependencies.Kudu, fork in Test := false)

lazy val mongodb = alpakkaProject("mongodb", "mongodb", Dependencies.MongoDb)

lazy val mqtt = alpakkaProject("mqtt", "mqtt", Dependencies.Mqtt)

lazy val orientdb = alpakkaProject("orientdb",
                                   "orientdb",
                                   Dependencies.OrientDB,
                                   fork in Test := true,
                                   parallelExecution in Test := false)

lazy val postgresqlcdc = alpakkaProject("postgresql-cdc", "postgresqlcdc", Dependencies.PostgreSqlCdc)

lazy val reference = alpakkaProject("reference", "reference", Dependencies.Reference, publish / skip := true)
  .disablePlugins(BintrayPlugin)

lazy val s3 = alpakkaProject("s3", "aws.s3", Dependencies.S3)

lazy val springWeb = alpakkaProject("spring-web", "spring.web", Dependencies.SpringWeb)

lazy val simpleCodecs = alpakkaProject("simple-codecs", "simplecodecs")

lazy val slick = alpakkaProject("slick", "slick", Dependencies.Slick)

lazy val sns = alpakkaProject("sns",
                              "aws.sns",
                              Dependencies.Sns,
                              // For mockito https://github.com/akka/alpakka/issues/390
                              parallelExecution in Test := false)

lazy val solr = alpakkaProject("solr", "solr", Dependencies.Solr, parallelExecution in Test := false)

lazy val sqs = alpakkaProject("sqs",
                              "aws.sqs",
                              Dependencies.Sqs,
                              // For mockito https://github.com/akka/alpakka/issues/390
                              parallelExecution in Test := false)

lazy val sse = alpakkaProject("sse", "sse", Dependencies.Sse)

lazy val text = alpakkaProject("text", "text")

lazy val udp = alpakkaProject("udp", "udp")

// FIXME: The exclude filter can be removed once we use JNR with
// https://github.com/jnr/jnr-enxio/pull/28 merged in.
lazy val unixdomainsocket = alpakkaProject(
  "unix-domain-socket",
  "unixdomainsocket",
  Dependencies.UnixDomainSocket,
  excludeFilter.in(unmanagedSources.in(headerCreate)) := HiddenFileFilter || "KQSelector.java"
)

lazy val xml = alpakkaProject("xml", "xml", Dependencies.Xml)

lazy val docs = project
  .enablePlugins(ParadoxPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .settings(
    name := "Alpakka",
    publish / skip := true,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxProperties ++= Map(
      "version" -> version.value,
      "scalaVersion" -> scalaVersion.value,
      "scalaBinaryVersion" -> scalaBinaryVersion.value,
      "akkaVersion" -> Dependencies.AkkaVersion,
      "akkaHttpVersion" -> Dependencies.AkkaHttpVersion,
      "extref.akka-docs.base_url" -> s"http://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s",
      "extref.akka-http-docs.base_url" -> s"http://doc.akka.io/docs/akka-http/${Dependencies.AkkaHttpVersion}/%s",
      "extref.java-api.base_url" -> "https://docs.oracle.com/javase/8/docs/api/index.html?%s.html",
      "extref.geode.base_url" -> "https://geode.apache.org/docs/guide/16/%s",
      "extref.javaee-api.base_url" -> "https://docs.oracle.com/javaee/7/api/index.html?%s.html",
      "extref.paho-api.base_url" -> "https://www.eclipse.org/paho/files/javadoc/index.html?%s.html",
      "extref.slick.base_url" -> s"https://slick.lightbend.com/doc/${Dependencies.SlickVersion}/%s",
      "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
      "javadoc.javax.jms.base_url" -> "https://docs.oracle.com/javaee/7/api/",
      "javadoc.akka.base_url" -> s"http://doc.akka.io/japi/akka/${Dependencies.AkkaVersion}/",
      "javadoc.akka.http.base_url" -> s"http://doc.akka.io/japi/akka-http/${Dependencies.AkkaHttpVersion}/",
      "javadoc.org.apache.hadoop.base_url" -> s"https://hadoop.apache.org/docs/r${Dependencies.HadoopVersion}/api/",
      "scaladoc.scala.base_url" -> s"http://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url" -> s"http://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
      "scaladoc.akka.http.base_url" -> s"https://doc.akka.io/api/akka-http/${Dependencies.AkkaHttpVersion}/",
      "scaladoc.akka.stream.alpakka.base_url" -> s"http://developer.lightbend.com/docs/api/alpakka/${version.value}"
    ),
    paradoxGroups := Map("Language" -> Seq("Scala", "Java")),
    paradoxLocalApiKey := "scaladoc.akka.stream.alpakka.base_url",
    paradoxLocalApiDir := (alpakka / Compile / sbtunidoc.BaseUnidocPlugin.autoImport.unidoc).value.head,
  )

lazy val `doc-examples` = project
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(BintrayPlugin, MimaPlugin)
  .dependsOn(
    modules.map(p => classpathDependency(p)): _*
  )
  .settings(
    name := s"akka-stream-alpakka-doc-examples",
    publish / skip := true,
    Dependencies.`Doc-examples`
  )

def alpakkaProject(projectId: String, moduleName: String, additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = projectId, base = file(projectId))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(
      name := s"akka-stream-alpakka-$projectId",
      AutomaticModuleName.settings(s"akka.stream.alpakka.$moduleName")
    )
    .settings(additionalSettings: _*)
