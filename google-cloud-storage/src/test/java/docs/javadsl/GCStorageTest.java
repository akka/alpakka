/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.stream.Attributes;
import akka.stream.alpakka.google.GoogleAttributes;
import akka.stream.alpakka.google.GoogleSettings;
import akka.stream.alpakka.googlecloud.storage.Bucket;
import akka.stream.alpakka.googlecloud.storage.StorageObject;
import akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage;
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorageWiremockBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class GCStorageTest extends GCStorageWiremockBase {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private final ActorSystem actorSystem = system();
  private final GoogleSettings sampleSettings = GoogleSettings.create(system());

  @After
  public void afterAll() {
    this.stopWireMockServer();
  }

  @Test
  public void createBucket() throws Exception {

    final String location = "europe-west1";

    mock().simulate(mockTokenApi(), mockBucketCreate(location));

    // #make-bucket

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Bucket> createBucketResponse =
        GCStorage.createBucket(bucketName(), location, actorSystem, sampleAttributes);
    final Source<Bucket, NotUsed> createBucketSourceResponse =
        GCStorage.createBucketSource(bucketName(), location);

    // #make-bucket

    final Bucket csBucket = createBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals("storage#bucket", csBucket.getKind());
    assertEquals(this.bucketName(), csBucket.getName());
    assertEquals(location.toUpperCase(), csBucket.getLocation());

    final Bucket srcBucket =
        createBucketSourceResponse
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals("storage#bucket", srcBucket.getKind());
    assertEquals(this.bucketName(), srcBucket.getName());
    assertEquals(location.toUpperCase(), srcBucket.getLocation());
  }

  @Test
  public void failWithErrorWhenBucketCreationFails() throws Exception {

    final String location = "europe-west1";

    mock().simulate(mockTokenApi(), mockBucketCreateFailure(location));

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Bucket> createBucketResponse =
        GCStorage.createBucket(this.bucketName(), location, actorSystem, sampleAttributes);
    final Source<Bucket, NotUsed> createBucketSourceResponse =
        GCStorage.createBucketSource(this.bucketName(), location);

    try {
      createBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Create failed", e.getMessage());
    }

    try {
      createBucketSourceResponse
          .runWith(Sink.head(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);

    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Create failed", e.getMessage());
    }
  }

  @Test
  public void deleteBucket() throws Exception {

    mock().simulate(mockTokenApi(), mockDeleteBucket());

    // #delete-bucket

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Done> deleteBucketResponse =
        GCStorage.deleteBucket(this.bucketName(), actorSystem, sampleAttributes);
    final Source<Done, NotUsed> deleteBucketSourceResponse =
        GCStorage.deleteBucketSource(this.bucketName());

    // #delete-bucket

    assertEquals(Done.done(), deleteBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals(
        Done.done(),
        deleteBucketSourceResponse
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenBucketDeletionFails() throws Exception {

    mock().simulate(mockTokenApi(), mockDeleteBucketFailure());

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Done> deleteBucketResponse =
        GCStorage.deleteBucket(this.bucketName(), actorSystem, sampleAttributes);
    final Source<Done, NotUsed> deleteBucketSourceResponse =
        GCStorage.deleteBucketSource(this.bucketName());

    try {
      deleteBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete failed", e.getMessage());
    }

    try {
      deleteBucketSourceResponse
          .runWith(Sink.head(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete failed", e.getMessage());
    }
  }

  @Test
  public void getBucketIfBucketExists() throws Exception {
    mock().simulate(mockTokenApi(), mockGetExistingBucket());

    // #get-bucket

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), actorSystem, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    // #get-bucket

    final Optional<Bucket> csBucket =
        getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertTrue(csBucket.isPresent());
    assertEquals(Optional.of(this.bucketName()), csBucket.map(Bucket::getName));

    final Optional<Bucket> bucketFromSource =
        getBucketSourceResponse
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertTrue(bucketFromSource.isPresent());
    assertEquals(Optional.of(this.bucketName()), bucketFromSource.map(Bucket::getName));
  }

  @Test
  public void doNotReturnBucketIfBucketDoesNotExist() throws Exception {
    mock().simulate(mockTokenApi(), mockGetNonExistingBucket());

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), actorSystem, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    assertEquals(
        Optional.empty(), getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals(
        Optional.empty(),
        getBucketSourceResponse
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenGettingBucketFails() throws Exception {
    mock().simulate(mockTokenApi(), mockGetBucketFailure());

    final Attributes sampleAttributes = GoogleAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), actorSystem, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    try {
      getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get bucket failed", e.getMessage());
    }

    try {
      getBucketSourceResponse
          .runWith(Sink.head(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get bucket failed", e.getMessage());
    }
  }

  @Test
  public void listEmptyBucket() throws Exception {
    mock().simulate(mockTokenApi(), mockEmptyBucketListing());

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertTrue(
        listSource
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listNonExistingFolder() throws Exception {
    final String folder = "folder";
    mock().simulate(mockTokenApi(), mockNonExistingFolderListing(folder));

    final Source<StorageObject, NotUsed> listSource =
        GCStorage.listBucket(this.bucketName(), folder);

    assertTrue(
        listSource
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listNonExistingBucket() throws Exception {
    mock().simulate(mockTokenApi(), mockNonExistingBucketListingJava());

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertTrue(
        listSource
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listExistingBucketUsingMultipleRequests() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";

    mock().simulate(mockTokenApi(), mockBucketListingJava(firstFileName, secondFileName));

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertEquals(
        Arrays.asList(firstFileName, secondFileName),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void listFolderInExistingBucketUsingMultipleRequests() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String folder = "folder";
    final boolean versions = true;

    mock()
        .simulate(
            mockTokenApi(),
            mockBucketListingJava(firstFileName, secondFileName, folder),
            mockBucketListingJava(firstFileName, secondFileName, folder, versions));

    // #list-bucket

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(bucketName(), folder);

    final Source<StorageObject, NotUsed> listVersionsSource =
        GCStorage.listBucket(bucketName(), folder, versions);

    // #list-bucket

    assertEquals(
        Arrays.asList(firstFileName, secondFileName),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));

    assertEquals(
        Arrays.asList(firstFileName, firstFileName + '#' + generation(), secondFileName),
        listVersionsSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenBucketListingFails() throws Exception {

    mock().simulate(mockTokenApi(), mockBucketListingFailure());

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(bucketName());

    try {
      listSource
          .map(StorageObject::name)
          .runWith(Sink.seq(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Bucket listing failed", e.getMessage());
    }
  }

  // This behavior is no longer supported, but keeping for the docs snippet
  // See https://github.com/akka/alpakka/pull/2613#discussion_r599046266
  //  @Test
  public void returnEmptySourceWhenListingBucketWithWrongSettings() throws Exception {

    mock().simulate(mockTokenApi(), mockBucketListingFailure());

    // #list-bucket-attributes

    final GoogleSettings newSettings = GoogleSettings.create(system()).withProjectId("projectId");

    final Source<StorageObject, NotUsed> listSource =
        GCStorage.listBucket(bucketName()).withAttributes(GoogleAttributes.settings(newSettings));

    // #list-bucket-attributes

    assertEquals(
        Arrays.asList(),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void getExistingStorageObject() throws Exception {
    mock()
        .simulate(
            mockTokenApi(),
            mockGetExistingStorageObjectJava(),
            mockGetExistingStorageObjectJava(generation()));

    // #objectMetadata

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    final Source<Optional<StorageObject>, NotUsed> getObjectGenerationSource =
        GCStorage.getObject(bucketName(), fileName(), generation());

    // #objectMetadata

    final Optional<StorageObject> storageObjectOpt =
        getObjectSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertTrue(storageObjectOpt.isPresent());

    final StorageObject storageObject = storageObjectOpt.get();
    assertEquals(fileName(), storageObject.name());
    assertEquals(bucketName(), storageObject.bucket());

    final Optional<StorageObject> storageObjectGenerationOpt =
        getObjectGenerationSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertTrue(storageObjectGenerationOpt.isPresent());

    final StorageObject storageObjectGeneration = storageObjectGenerationOpt.get();
    assertEquals(fileName(), storageObjectGeneration.name());
    assertEquals(bucketName(), storageObjectGeneration.bucket());
    assertEquals(generation(), storageObjectGeneration.generation());
  }

  @Test
  public void getNoneIfStorageObjectDoesNotExist() throws Exception {
    mock().simulate(mockTokenApi(), mockGetNonExistingStorageObject());

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    assertEquals(
        Optional.empty(),
        getObjectSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenGetStorageObjectFails() throws Exception {
    mock().simulate(mockTokenApi(), mockGetNonStorageObjectFailure());

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    try {
      getObjectSource.runWith(Sink.head(), system()).toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get storage object failed", e.getMessage());
    }
  }

  @Test
  public void downloadFileWhenFileExists() throws Exception {
    final String fileContent = "Google storage file content";
    final String fileContentGeneration = "Google storage file content (archived)";

    mock()
        .simulate(
            mockTokenApi(),
            mockFileDownloadJava(fileContent),
            mockFileDownloadJava(fileContentGeneration, generation()));

    // #download

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadGenerationSource =
        GCStorage.download(bucketName(), fileName(), generation());

    // #download

    final Source<ByteString, NotUsed> data =
        downloadSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final Source<ByteString, NotUsed> dataGeneration =
        downloadGenerationSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final CompletionStage<List<String>> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.seq(), system());

    final CompletionStage<List<String>> resultGenerationCompletionStage =
        dataGeneration.map(ByteString::utf8String).runWith(Sink.seq(), system());

    final List<String> result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final List<String> resultGeneration =
        resultGenerationCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final String content = String.join("", result);
    final String contentGeneration = String.join("", resultGeneration);

    assertEquals(fileContent, content);
    assertEquals(fileContentGeneration, contentGeneration);
  }

  @Test
  public void downloadResultsInNoneWhenFileDoesNotExist() throws Exception {

    mock().simulate(mockTokenApi(), mockNonExistingFileDownload());

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    assertEquals(
        Optional.empty(),
        downloadSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenFileDownloadFails() throws Exception {

    mock().simulate(mockTokenApi(), mockFileDownloadFailure());

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    try {
      downloadSource.runWith(Sink.head(), system()).toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] File download failed", e.getMessage());
    }
  }

  @Test
  public void retryWhenFileDownloadFailsWithServerError() throws Exception {

    final String fileContent = "SomeFileContent";

    mock()
        .simulate(
            mockTokenApi(),
            mockFileDownloadFailureThenSuccess(500, "Internal server error", fileContent));

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    Source<ByteString, NotUsed> data =
        downloadSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(500, TimeUnit.SECONDS)
            .get();

    final CompletionStage<List<String>> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.seq(), system());

    final List<String> result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final String content = String.join("", result);

    assertEquals(fileContent, content);
  }

  @Test
  public void uploadSmallFile() throws Exception {
    final String fileContent = "chunk1";
    final ContentType contentType = ContentTypes.APPLICATION_OCTET_STREAM;
    final Source<ByteString, NotUsed> fileSource =
        Source.single(ByteString.fromString(fileContent));

    mock().simulate(mockTokenApi(), mockUploadSmallFile(fileContent));

    final Source<StorageObject, NotUsed> simpleUploadSource =
        GCStorage.simpleUpload(bucketName(), fileName(), fileSource, contentType);

    final StorageObject storageObject =
        simpleUploadSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(fileName(), storageObject.name());
    assertEquals(bucketName(), storageObject.bucket());
  }

  @Test
  public void failWithErrorWhenSmallFileUploadFails() throws Exception {
    final String fileContent = "chunk1";
    final ContentType contentType = ContentTypes.APPLICATION_OCTET_STREAM;
    final Source<ByteString, NotUsed> fileSource =
        Source.single(ByteString.fromString(fileContent));

    mock().simulate(mockTokenApi(), mockUploadSmallFileFailure(fileContent));

    final Source<StorageObject, NotUsed> simpleUploadSource =
        GCStorage.simpleUpload(bucketName(), fileName(), fileSource, contentType);

    try {
      simpleUploadSource
          .runWith(Sink.head(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Upload small file failed", e.getMessage());
    }
  }

  @Test
  public void deleteExistingObject() throws Exception {
    mock()
        .simulate(
            mockTokenApi(),
            mockDeleteObjectJava(fileName()),
            mockDeleteObjectJava(fileName(), generation()));

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());
    final Source<Boolean, NotUsed> deleteGenerationSource =
        GCStorage.deleteObject(bucketName(), fileName(), generation());

    assertTrue(
        deleteSource.runWith(Sink.head(), system()).toCompletableFuture().get(5, TimeUnit.SECONDS));

    assertTrue(
        deleteGenerationSource
            .runWith(Sink.head(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void doNotDeleteNonExistingObject() throws Exception {
    mock().simulate(mockTokenApi(), mockNonExistingDeleteObject(fileName()));

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());

    assertFalse(
        deleteSource.runWith(Sink.head(), system()).toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWhenDeleteObjectFails() throws Exception {
    mock().simulate(mockTokenApi(), mockDeleteObjectFailure(fileName()));

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());

    try {
      deleteSource.runWith(Sink.head(), system()).toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete object failed", e.getMessage());
    }
  }

  @Test
  public void deleteExistingFolder() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String prefix = "folder";

    mock()
        .simulate(
            mockTokenApi(),
            mockBucketListingJava(firstFileName, secondFileName, prefix),
            mockDeleteObjectJava(firstFileName),
            mockDeleteObjectJava(secondFileName));

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    assertEquals(
        Arrays.asList(true, true),
        deleteObjectsByPrefixSource
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void doNotDeleteNonExistingFolder() throws Exception {
    final String prefix = "folder";

    mock()
        .simulate(
            mockTokenApi(),
            mockNonExistingBucketListingJava(prefix),
            mockObjectDoesNotExist(prefix));

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    assertEquals(
        Arrays.asList(),
        deleteObjectsByPrefixSource
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWhenFolderDeleteFails() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String prefix = "folder";

    mock()
        .simulate(
            mockTokenApi(),
            mockNonExistingBucketListingJava(prefix),
            mockBucketListingJava(firstFileName, secondFileName, prefix),
            mockDeleteObjectJava(firstFileName),
            mockDeleteObjectFailure(secondFileName));

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    try {
      deleteObjectsByPrefixSource
          .runWith(Sink.seq(), system())
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete object failed", e.getMessage());
    }
  }

  @Test
  public void uploadLargeFile() throws Exception {
    final int chunkSize = 256 * 1024;
    final String firstChunkContent = this.getRandomString(chunkSize);
    final String secondChunkContent = this.getRandomString(chunkSize);

    mock()
        .simulate(
            mockTokenApi(), mockLargeFileUpload(firstChunkContent, secondChunkContent, chunkSize));

    // #upload

    final Sink<ByteString, CompletionStage<StorageObject>> sink =
        GCStorage.resumableUpload(
            bucketName(), fileName(), ContentTypes.TEXT_PLAIN_UTF8, chunkSize);

    final Source<ByteString, NotUsed> source =
        Source.from(
            Arrays.asList(
                ByteString.fromString(firstChunkContent),
                ByteString.fromString(secondChunkContent)));

    final CompletionStage<StorageObject> result = source.runWith(sink, system());

    // #upload

    final StorageObject storageObject = result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(fileName(), storageObject.name());
    assertEquals(bucketName(), storageObject.bucket());
  }

  //  @Test The new ResumableUpload API automatically resumes interrupted/failed uploads
  public void failWithErrorWhenLargeFileUploadFails() throws Exception {
    final int chunkSize = 256 * 1024;
    final String firstChunkContent = this.getRandomString(chunkSize);
    final String secondChunkContent = this.getRandomString(chunkSize);

    mock()
        .simulate(
            mockTokenApi(),
            mockLargeFileUploadFailure(firstChunkContent, secondChunkContent, chunkSize));

    final Sink<ByteString, CompletionStage<StorageObject>> sink =
        GCStorage.resumableUpload(
            bucketName(), fileName(), ContentTypes.TEXT_PLAIN_UTF8, chunkSize);

    final Source<ByteString, NotUsed> source =
        Source.from(
            Arrays.asList(
                ByteString.fromString(firstChunkContent),
                ByteString.fromString(secondChunkContent)));

    try {
      source.runWith(sink, system()).toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals(
          "akka.stream.alpakka.googlecloud.storage.FailedUpload: Uploading part failed with status 400 Bad Request: Chunk upload failed",
          e.getMessage());
    }
  }

  @Test
  public void rewriteFile() throws Exception {
    final String rewriteBucketName = "alpakka-rewrite";

    mock().simulate(mockTokenApi(), mockRewrite(rewriteBucketName));

    // #rewrite

    final CompletionStage<StorageObject> result =
        GCStorage.rewrite(bucketName(), fileName(), rewriteBucketName, fileName()).run(system());

    // #rewrite

    final StorageObject storageObject = result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(fileName(), storageObject.name());
    assertEquals(rewriteBucketName, storageObject.bucket());
  }

  @Test
  public void failWhenRewriteFileFails() throws Exception {
    final String rewriteBucketName = "alpakka-rewrite";

    mock().simulate(mockTokenApi(), mockRewriteFailure(rewriteBucketName));

    final CompletionStage<StorageObject> result =
        GCStorage.rewrite(bucketName(), fileName(), rewriteBucketName, fileName()).run(system());

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Rewrite failed", e.getMessage());
    }
  }
}
