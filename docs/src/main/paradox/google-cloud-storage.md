# Google Cloud Storage

Google Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.

Further information at the official [Google Cloud Storage documentation website](https://cloud.google.com/storage/docs/).
This connector communicates to Cloud Storage via HTTP requests.

@@project-info{ projectId="google-cloud-storage" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-storage_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-cloud-storage" }

## Configuration

The settings for the Google Cloud Storage connector are read by default from `alpakka.googlecloud.storage` configuration section.
If you use a non-standard configuration path or need multiple different configurations, please refer to @ref[the attributes section below](google-cloud-storage.md#apply-google-cloud-storage-settings-to-a-part-of-the-stream) to see how to apply different configuration to different parts of the stream.
You'll first need to prepare your credentials for access to google cloud storage.
All of the available configuration settings can be found in the @github[application.conf](/google-cloud-storage/src/test/resources/application.conf).

HOCON:
: @@snip [snip](/google-cloud-storage/src/test/resources/application.conf) { #settings }


## Store a file in Google Cloud Storage

A file can be uploaded to Google Cloud Storage by creating a source of @scala[@scaladoc[ByteString](akka.util.ByteString)]@java[@javadoc[ByteString](akka.util.ByteString)] and running that with a sink created from @scala[@scaladoc[GCStorage.resumableUpload](akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage$)]@java[@scaladoc[GCStorage.resumableUpload](akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage$)].

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSinkSpec.scala) { #upload }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #upload }

## Download a file from Google Cloud Storage

A source for downloading a file can be created by calling @scala[@scaladoc[GCStorage.download](akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage$)]@java[@scaladoc[GCStorage.download](akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage$)].
It will emit an @scala[`Option`]@java[`Optional`] that will hold file's data or will be empty if no such file can be found.

If you need to download the specific version of the object in a bucket where object versioning is enabled, you can specify the `generation`.

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #download }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #download }


## Access object metadata without downloading object from Google Cloud Storage

If you do not need object itself, you can query for only object metadata using a source from @scala[@scaladoc[GCStorage.getObject](akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage$)]@java[@scaladoc[GCStorage.getObject](akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage$)].

If you need the specific version of the object metadata in a bucket where object versioning is enabled, you can specify the `generation`. 

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #objectMetadata }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #objectMetadata }

## List bucket contents

To get a list of all objects in a bucket, use @scala[@scaladoc[GCStorage.listBucket](akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage$)]@java[@scaladoc[GCStorage.listBucket](akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage$)].
When run, this will give a stream of @scaladoc[StorageObject](akka.stream.alpakka.googlecloud.storage.StorageObject).

To get a list of both live and archived versions of all objects in a bucket where object versioning is enabled, the `versions` has to be set to `true`

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #list-bucket }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #list-bucket }

## Rewrite (multi part)

Copy an Google Clouds Storage object from source bucket to target bucket using @scala[@scaladoc[GCStorage.rewrite](akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage$)]@java[@scaladoc[GCStorage.rewrite](akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage$)].
When run, this will emit a single @scaladoc[StorageObject](akka.stream.alpakka.googlecloud.storage.StorageObject) with the information about the copied object.

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSinkSpec.scala) { #rewrite }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #rewrite }

## Apply Google Cloud Storage settings to a part of the stream

It is possible to make one part of the stream use different @scaladoc[GCStorageSettings](akka.stream.alpakka.googlecloud.storage.GCStorageSettings) from the rest of the graph.
This can be useful, when one stream is used to copy files across regions with different service accounts.
You can attach a custom `GCStorageSettings` instance or a custom config path to a graph using attributes from @scaladoc[GCStorageAttributes](akka.stream.alpakka.googlecloud.storage.GCStorageAttributes$):

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #list-bucket-attributes }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #list-bucket-attributes }


## Bucket management

Bucket management API provides functionality for both Sources and Futures / CompletionStages. 
In case of the Future API user can specify attributes to the request in the method itself and as for Sources it can be done via method `.withAttributes`.
For more information about attributes see: @scaladoc[GCStorageAttributes](akka.stream.alpakka.googlecloud.storage.GCStorageAttributes$) and @scaladoc[Attributes](akka.stream.Attributes)

### Make bucket
In order to create a bucket in Google Cloud Storage you need to specify it's unique name. This value has to be set accordingly to the [requirements](https://cloud.google.com/storage/docs/naming).
The bucket will be created in the given location.

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #make-bucket }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #make-bucket }


### Delete bucket
To delete a bucket you need to specify its name and the bucket needs to be empty.

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #delete-bucket }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #delete-bucket }


### Get bucket
To get a bucket you need to specify its name.

Scala
: @@snip [snip](/google-cloud-storage/src/test/scala/docs/scaladsl/GCStorageSourceSpec.scala) { #get-bucket }

Java
: @@snip [snip](/google-cloud-storage/src/test/java/docs/javadsl/GCStorageTest.java) { #get-bucket }


## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > google-cloud-storage/test
    ```

Java
:   ```
    sbt
    > google-cloud-storage/test
    ```

> Some test code requires access to Google cloud storage, to run them you will need to configure a project and pub/sub in google cloud and provide your own credentials.
