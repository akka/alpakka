// Configuration for creating changelogs with gren
// https://github.com/github-tools/github-release-notes/
// run: gren changelog  --generate --override --tags=v0.20..v1.0-RC1
module.exports = {
    "dataSource": "prs",
    "prefix": "",
    "onlyMilestones": true,
  "groupBy": {
    "Infrastructure": ["alpakka-itself"],
    "AMQP": ["p:amqp"],
    "Apache Solr": ["p:solr"],
    "Apache HDFS": ["p:hdfs"],
    "Apache Kudu": ["p:kudu"],
    "AvroParquet": ["p:avroparquet"],
    "AWS Lambda": ["p:aws-lambda"],
    "AWS Kinesis": ["p:kinesis"],
    "AWS S3": ["p:aws-s3"],
    "AWS SNS": ["p:aws-sns"],
    "AWS SQS": ["p:aws-sqs"],
    "Azure Storage Queue": ["p:azure-storage-queue"],
    "Cassandra": ["p:cassandra"],
    "Comma-separated Values (CSV)": ["p:csv"],
    "Couchbase": ["p:couchbase"],
    "Dynamo DB": ["p:dynamodb"],
    "Elasticsearch": ["p:elasticsearch"],
    "File": ["p:file"],
    "FTP": ["p:ftp"],
    "Google Cloud Pub/Sub": ["p:google-cloud-pub-sub"],
    "Google Cloud Pub/Sub gRPC": ["p:google-cloud-pub-sub-grpc"],
    "Google Firebase": ["p:google-fcm"],
    "Geode": ["p:geode"],
    "HBase": ["p:hbase"],
    "IronMQ": ["p:ironmq"],
    "JMS": ["p:jms"],
    "JDBC": ["p:jdbc"],
    "Json streaming": ["p:json-streaming"],
    "MongoDB": ["p:mongodb"],
    "MQTT": ["p:mqtt"],
    "MQTT Streaming": ["p:mqtt-streaming"],
    "OrientDB": ["p:orientdb"],
    "Reference": ["p:reference"],
    "Server-sent events (SSE)": ["p:sse"],
    "Simple Codecs": ["p:recordio"],
    "Slick": ["p:slick"],
    "Spring Web": ["p:spring-web"],
    "Text": ["p:text"],
    "UDP": ["p:udp"],
    "Unix Domain Socket": ["p:unix-domain-socket"],
    "XML": ["p:xml"],
    "Other": ["api-change", "bug", "dependency-change", "enhancement"]
  },
    "changelogFilename": "CHANGELOG.md",
    "template": {
      // https://github.com/github-tools/github-release-notes/blob/master/lib/src/Gren.js#L512
      "issue": ({ name, text, url, user_login, user_url, labels, body }) =>
        `* ${name} [${text}](${url}) ${user_login ? `by [@${user_login}](${user_url})` : ``} ${labels}\n${body ? `<!------\n${body}\n--->\n` : ``}`,
      label: '**{{label}}** ',
    }
}
