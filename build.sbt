lazy val modules: Seq[ProjectReference] = Seq(
//  amqp,
//  awslambda,
//  azureStorageQueue,
//  cassandra,
//  csv,
//  dynamodb,
//  elasticsearch,
//  files,
//  ftp,
//  geode,
//  googleCloudPubSub,
//  hbase,
//  ironmq,
//  jms,
//  kinesis,
//  mongodb,
//  mqtt,
//  orientdb,
  s3,
//  springWeb,
//  simpleCodecs,
//  slick,
//  sns,
//  solr,
//  sqs,
//  sse,
//  unixdomainsocket,
//  xml
)

lazy val alpakka = project
  .in(file("."))
  .enablePlugins(PublishUnidoc)
  .aggregate(modules: _*)
  .aggregate(`doc-examples`)
  .settings(
    onLoadMessage :=
      """
        |** Welcome to the sbt build definition for Alpakka! **
        |
        |Useful sbt tasks:
        |
        |  docs/local:paradox - builds documentation with locally
        |    linked Scala API docs, which can be found at
        |    docs/target/paradox/site/local
        |
        |  test - runs all the tests for all of the connectors.
        |   Make sure to run `docker-compose up` first.
        |
        |  mqtt/testOnly *.MqttSourceSpec - runs a single test
      """.stripMargin
  )

lazy val amqp = alpakkaProject("amqp", Dependencies.Amqp)

lazy val awslambda = alpakkaProject("awslambda",
                                    Dependencies.AwsLambda,
                                    // For mockito https://github.com/akka/alpakka/issues/390
                                    parallelExecution in Test := false)

lazy val azureStorageQueue = alpakkaProject("azure-storage-queue", Dependencies.AzureStorageQueue)

lazy val cassandra = alpakkaProject("cassandra", Dependencies.Cassandra)

lazy val csv = alpakkaProject("csv", Dependencies.Csv)

lazy val dynamodb = alpakkaProject("dynamodb", Dependencies.DynamoDB)

lazy val elasticsearch = alpakkaProject(
  "elasticsearch",
  Dependencies.Elasticsearch,
  // For elasticsearch-cluster-runner https://github.com/akka/alpakka/issues/479
  parallelExecution in Test := false
)

// The name 'file' is taken by `sbt.file`, hence 'files'
lazy val files = alpakkaProject("file", Dependencies.File)

lazy val ftp = alpakkaProject(
  "ftp",
  Dependencies.Ftp,
  parallelExecution in Test := false,
  fork in Test := true,
  // To avoid potential blocking in machines with low entropy (default is `/dev/random`)
  javaOptions in Test += "-Djava.security.egd=file:/dev/./urandom"
)

lazy val geode = alpakkaProject("geode", Dependencies.Geode, fork in Test := true, parallelExecution in Test := false)

lazy val googleCloudPubSub = alpakkaProject(
  "google-cloud-pub-sub",
  Dependencies.GooglePubSub,
  fork in Test := true,
  envVars in Test := Map("PUBSUB_EMULATOR_HOST" -> "localhost:8538"),
  // For mockito https://github.com/akka/alpakka/issues/390
  parallelExecution in Test := false
)

lazy val hbase = alpakkaProject("hbase", Dependencies.HBase, fork in Test := true)

lazy val ironmq = alpakkaProject("ironmq", Dependencies.IronMq)

lazy val jms = alpakkaProject("jms", Dependencies.Jms, parallelExecution in Test := false)

lazy val kinesis = alpakkaProject("kinesis",
                                  Dependencies.Kinesis,
                                  // For mockito https://github.com/akka/alpakka/issues/390
                                  parallelExecution in Test := false)

lazy val mongodb = alpakkaProject("mongodb", Dependencies.MongoDb)

lazy val mqtt = alpakkaProject("mqtt", Dependencies.Mqtt)

lazy val orientdb =
  alpakkaProject("orientdb", Dependencies.OrientDB, fork in Test := true, parallelExecution in Test := false)

lazy val s3 = alpakkaProject("s3", Dependencies.S3)

lazy val springWeb = alpakkaProject("spring-web", Dependencies.SpringWeb)

lazy val simpleCodecs = alpakkaProject("simple-codecs")

lazy val slick = alpakkaProject("slick", Dependencies.Slick)

lazy val sns = alpakkaProject("sns",
                              Dependencies.Sns,
                              // For mockito https://github.com/akka/alpakka/issues/390
                              parallelExecution in Test := false)

lazy val solr = alpakkaProject("solr", Dependencies.Solr, parallelExecution in Test := false)

lazy val sqs = alpakkaProject("sqs",
                              Dependencies.Sqs,
                              // For mockito https://github.com/akka/alpakka/issues/390
                              parallelExecution in Test := false)

lazy val sse = alpakkaProject("sse", Dependencies.Sse)

lazy val unixdomainsocket = alpakkaProject("unix-domain-socket", Dependencies.UnixDomainSocket)

lazy val xml = alpakkaProject("xml", Dependencies.Xml)

val Local = config("local")
val defaultParadoxSettings: Seq[Setting[_]] = Seq(
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
    "extref.javaee-api.base_url" -> "https://docs.oracle.com/javaee/7/api/index.html?%s.html",
    "extref.paho-api.base_url" -> "https://www.eclipse.org/paho/files/javadoc/index.html?%s.html",
    "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
    "javadoc.javax.jms.base_url" -> "https://docs.oracle.com/javaee/7/api/",
    "javadoc.akka.base_url" -> s"http://doc.akka.io/japi/akka/${Dependencies.AkkaVersion}/",
    "javadoc.akka.http.base_url" -> s">http://doc.akka.io/japi/akka-http/${Dependencies.AkkaHttpVersion}/",
    "scaladoc.scala.base_url" -> s"http://www.scala-lang.org/api/current/",
    "scaladoc.akka.base_url" -> s"http://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
    "scaladoc.akka.http.base_url" -> s"https://doc.akka.io/api/akka-http/${Dependencies.AkkaHttpVersion}/",
    "scaladoc.akka.stream.alpakka.base_url" -> s"http://developer.lightbend.com/docs/api/alpakka/${version.value}",
    "snip.alpakka.base_dir" -> (baseDirectory in ThisBuild).value.getAbsolutePath
  ),
  sourceDirectory := baseDirectory.value / "src" / "main"
)

lazy val docs = project
  .enablePlugins(ParadoxPlugin, NoPublish)
  .disablePlugins(BintrayPlugin)
  .settings(
    name := "Alpakka",
    inConfig(Compile)(defaultParadoxSettings),
    ParadoxPlugin.paradoxSettings(Local),
    inConfig(Local)(defaultParadoxSettings),
    paradoxGroups := Map("Language" -> Seq("Scala", "Java")),
    paradoxProperties in Local ++= Map(
      // point API doc links to locally generated API docs
      "scaladoc.akka.stream.alpakka.base_url" -> sbt.io.Path
        .rebase(
          (baseDirectory in ThisBuild).value,
          "../../../../../"
        )((sbtunidoc.BaseUnidocPlugin.autoImport.unidoc in alpakka in Compile).value.head)
        .get
    )
  )

lazy val `doc-examples` = project
  .enablePlugins(NoPublish, AutomateHeaderPlugin)
  .disablePlugins(BintrayPlugin)
  .dependsOn(
    modules.map(p => classpathDependency(p)): _*
  )
  .settings(
    name := s"akka-stream-alpakka-doc-examples",
    Dependencies.`Doc-examples`
  )

def alpakkaProject(projectId: String, additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = projectId, base = file(projectId))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(
      name := s"akka-stream-alpakka-$projectId"
    )
    .settings(additionalSettings: _*)
