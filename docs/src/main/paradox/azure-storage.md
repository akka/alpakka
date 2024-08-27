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
artifact=akka-stream-alpakka-s3_$scala.binary.version$
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

* `authorization-type`, this is the type of authorization to use as described [here](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage), possible values are `anon`, `SharedKey`, `SharedKeyLite`, or `sas`. Environment variable `AZURE_STORAGE_AUTHORIZATION_TYPE` can be set to override this configuration.
* `account-name`, this is the name of the blob storage or file share. Environment variable `AZURE_STORAGE_ACCOUNT_NAME` can be set to override this configuration.
* `account-key`, Account key to use to create authorization signature, mandatory for `SharedKey` or `SharedKeyLite` authorization types, as described [here](https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key). Environment variable `AZURE_STORAGE_ACCOUNT_KEY` can be set to override this configuration.
* `sas-token` if authorization type is `sas`. Environment variable `AZURE_STORAGE_SAS_TOKEN` can be set to override this configuration.

## Supported operations on Blob service

### Create Container

The [`Create Container`](https://learn.microsoft.com/en-us/rest/api/storageservices/create-container) operation creates a new container under the specified account.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-container }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-container }

### Put Blob

The [`Put Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob) operation creates a new block, page, or append blob, or updates the content of an existing block blob.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #put-blob }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #put-blob }

### Get Blob

The [`Get Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob) operation reads or downloads a blob from the system, including its metadata and properties.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob }

In order to download a range of a file's data you can use overloaded method which additionally takes `ByteRange` as argument.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-range }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-range }

### Get blob properties without downloading blob

The [`Get Blob Properties`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties) operation returns all user-defined metadata, standard HTTP properties, and system properties for the blob. (**Note:** Current implementation does not return user-defined metatdata.)

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-properties }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-blob-properties }

### Delete Blob

The [`Delete Blob`](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob) operation deletes the specified blob.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delete-blob }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delte-blob }

## Supported operations on File service

### Create File

The [`Create File`](https://learn.microsoft.com/en-us/rest/api/storageservices/create-file) operation creates a new file or replaces a file.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-file }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #create-file }

### Update Range

The [`Update Range`](https://learn.microsoft.com/en-us/rest/api/storageservices/put-range) operation writes a range of bytes to a file.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #update-range }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #update-range }

Range can be cleared using `ClearRange` function.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #clear-range }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #clear-range }

### Get File

The [`Get File`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-file) operation reads or downloads a file from the system, including its metadata and properties.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file }

### Get file properties without downloading blob

The [`Get File Properties`](https://learn.microsoft.com/en-us/rest/api/storageservices/get-file-properties) operation returns all user-defined metadata, standard HTTP properties, and system properties for the file. (**Note:** Current implementation does not return user-defined metatdata.)

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file-properties }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #get-file-properties }

### Delete Blob

The [`Delete File`](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-file) operation immediately removes the file from the storage account.

Scala
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delete-file }

Java
: @@snip [snip](/azure-storage/src/test/scala/docs/scaladsl/StorageSpec.scala) { #delte-file }
