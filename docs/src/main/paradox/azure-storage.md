# Azure Storage

Azure Storage connector provides Akka Stream Source for Azure Storage. Currently only supports `Blob` and `File` services. For detail about these services please read [Azure docs](https://learn.microsoft.com/en-us/rest/api/storageservices/).

@@project-info{ projectId="azure-storage" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-stream-alpakka-azure-storage_$scala.binary.version$
version=$project.version$
symbol2=AkkaVersion
value2=$akka.version$
group2=com.typesafe.akka
artifact2=akka-stream_$scala.binary.version$
version2=AkkaVersion
symbol3=AkkaHttpVersion
value3=$akka-http.version$
group3=com.typesafe.akka
artifact3=akka-http_$scala.binary.version$
version3=AkkaHttpVersion
group4=com.typesafe.akka
artifact4=akka-http-xml_$scala.binary.version$
version4=AkkaHttpVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="azure-storage" }

## Configuration

The settings for the Azure Storage connector are read by default from `alpakka.azure-storage` configuration section. Credentials are defined in `credentials` section of [`reference.conf`](/azure-storage/src/main/resources/reference.conf).

Scala
: @@snip [snip](/azure-storage/src/main/resources/reference.conf) { #azure-credentials }

Java
: @@snip [snip](/azure-storage/src/main/resources/reference.conf) { #azure-credentials }

At minimum following configurations needs to be set:

* `authorization-type`, this is the type of authorization to use as described [here](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage), possible values are `anon`, `SharedKey`, or `sas`. Environment variable `AZURE_STORAGE_AUTHORIZATION_TYPE` can be set to override this configuration.
* `account-name`, this is the name of the blob storage or file share. Environment variable `AZURE_STORAGE_ACCOUNT_NAME` can be set to override this configuration.
* `account-key`, Account key to use to create authorization signature, mandatory for `SharedKey` or `SharedKeyLite` authorization types, as described [here](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key). Environment variable `AZURE_STORAGE_ACCOUNT_KEY` can be set to override this configuration.
* `sas-token` if authorization type is `sas`. Environment variable `AZURE_STORAGE_SAS_TOKEN` can be set to override this configuration.

## Building request

Each function takes two parameters `objectPath` and `requestBuilder`. The `objectPath` is a `/` separated string of the path of the blob
or file, for example, `my-container/my-blob` or `my-share/my-directory/my-file`.

Each request builder is subclass of [`RequestBuilder`](/azure-storage/src/main/scala/akka/stream/alpakka/azure/storage/requests/RequestBuilder.scala) which knows how to construct request for the given operation.

### Create simple request builder with default values

In this example `GetBlob` builder is initialized without any optional field.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/RequestBuilderSpec.scala) { #simple-request-builder }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/RequestBuilderTest.java) { #simple-request-builder }

### Create request builder initialized with optional fields

In this example `GetBlob` builder is initialized with given `leaseId` and `range` fields.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/RequestBuilderSpec.scala) { #populate-request-builder }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/RequestBuilderTest.java) { #populate-request-builder }

### Create request builder initialized with required fields

In this example `CreateFile` builder is initialized with `maxFileSize` and `contentType` fields, which are required fields for `CreateFile` operation.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/RequestBuilderSpec.scala) { #request-builder-with-initial-values }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/RequestBuilderTest.java) { #request-builder-with-initial-values }

### Create request builder with ServerSideEncryption

`ServerSideEncryption` can be initialized in similar fashion.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/RequestBuilderSpec.scala) { #request-builder-with-sse }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/RequestBuilderTest.java) { #request-builder-with-sse }

###  Create request builder with additional headers

Some operations allow you to add additional headers, for `GetBlob` you can specify `If-Match` header, which specify this header to perform the operation only if the resource's ETag matches the value specified, this can be done by calling `addHeader` function.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/RequestBuilderSpec.scala) { #request-builder-with-additional-headers }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/RequestBuilderTest.java) { #request-builder-with-additional-headers }

## Supported operations on Blob service

### Create Container

The [`Create Container`](https://learn.microsoft.com/en-us/rest/api/storageservices/create-container) operation creates a new container under the specified account.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-container }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #create-container }

### Put Block Blob

The [`Put Block Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob) operation creates a new block or updates the content of an existing block blob.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #put-block-blob }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #put-block-blob }

### Get Blob

The [`Get Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob) operation reads or downloads a blob from the system, including its metadata and properties.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #get-blob }

In order to download a range of a file's data you can use overloaded method which additionally takes `ByteRange` as argument.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-range }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #get-blob-range }

### Get blob properties without downloading blob

The [`Get Blob Properties`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties) operation returns all user-defined metadata, standard HTTP properties, and system properties for the blob. (**Note:** Current implementation does not return user-defined metadata.)

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-properties }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #get-blob-properties }

### Delete Blob

The [`Delete Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob) operation deletes the specified blob.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delete-blob }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #delete-blob }

## Supported operations on File service

### Create File

The [`Create File`](https://learn.microsoft.com/en-us/rest/api/storageservices/create-file) operation creates a new file or replaces a file.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-file }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #create-file }

### Update Range

The [`Update Range`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-range) operation writes a range of bytes to a file.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #update-range }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #update-range }

Range can be cleared using `ClearRange` function.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #clear-range }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #clear-range }

### Get File

The [`Get File`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-file) operation reads or downloads a file from the system, including its metadata and properties.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #get-file }

### Get file properties without downloading file

The [`Get File Properties`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-file-properties) operation returns all user-defined metadata, standard HTTP properties, and system properties for the file. (**Note:** Current implementation does not return user-defined metatdata.)

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file-properties }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #get-file-properties }

### Delete Blob

The [`Delete File`](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-file2) operation immediately removes the file from the storage account.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delete-file }

Java
: @@snip [snip](/azure-storage/src/test/java/docs/javadsl/StorageTest.java) { #delete-file }
