/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.Materializer;
import akka.stream.alpakka.googlecloud.storage.*;
import akka.stream.alpakka.googlecloud.storage.javadsl.GCStorage;
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorageWiremockBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class GCStorageTest extends GCStorageWiremockBase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private final Materializer materializer = ActorMaterializer.create(system());
  private final GCStorageSettings sampleSettings = GCStorageExt.get(system()).settings();

  @After
  public void afterAll() {
    this.stopWireMockServer();
  }

  @Test
  public void createBucket() throws Exception {
    this.mockTokenApi();

    final String location = "europe-west1";

    mockBucketCreate(location);

    // #make-bucket

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Bucket> createBucketResponse =
        GCStorage.createBucket(bucketName(), location, materializer, sampleAttributes);
    final Source<Bucket, NotUsed> createBucketSourceResponse =
        GCStorage.createBucketSource(bucketName(), location);

    // #make-bucket

    final Bucket csBucket = createBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals("storage#bucket", csBucket.getKind());
    assertEquals(this.bucketName(), csBucket.getName());
    assertEquals(location.toUpperCase(), csBucket.getLocation());

    final Bucket srcBucket =
        createBucketSourceResponse
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals("storage#bucket", srcBucket.getKind());
    assertEquals(this.bucketName(), srcBucket.getName());
    assertEquals(location.toUpperCase(), srcBucket.getLocation());
  }

  @Test
  public void failWithErrorWhenBucketCreationFails() throws Exception {
    this.mockTokenApi();

    final String location = "europe-west1";

    this.mockBucketCreateFailure(location);

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Bucket> createBucketResponse =
        GCStorage.createBucket(this.bucketName(), location, materializer, sampleAttributes);
    final Source<Bucket, NotUsed> createBucketSourceResponse =
        GCStorage.createBucketSource(this.bucketName(), location);

    try {
      createBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Create failed", e.getMessage());
    }

    try {
      createBucketSourceResponse
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);

    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Create failed", e.getMessage());
    }
  }

  @Test
  public void deleteBucket() throws Exception {
    this.mockTokenApi();

    this.mockDeleteBucket();

    // #delete-bucket

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Done> deleteBucketResponse =
        GCStorage.deleteBucket(this.bucketName(), materializer, sampleAttributes);
    final Source<Done, NotUsed> deleteBucketSourceResponse =
        GCStorage.deleteBucketSource(this.bucketName());

    // #delete-bucket

    assertEquals(Done.done(), deleteBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals(
        Done.done(),
        deleteBucketSourceResponse
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenBucketDeletionFails() throws Exception {
    this.mockTokenApi();
    this.mockDeleteBucketFailure();

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Done> deleteBucketResponse =
        GCStorage.deleteBucket(this.bucketName(), materializer, sampleAttributes);
    final Source<Done, NotUsed> deleteBucketSourceResponse =
        GCStorage.deleteBucketSource(this.bucketName());

    try {
      deleteBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete failed", e.getMessage());
    }

    try {
      deleteBucketSourceResponse
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete failed", e.getMessage());
    }
  }

  @Test
  public void getBucketIfBucketExists() throws Exception {
    this.mockTokenApi();
    this.mockGetExistingBucket();

    // #get-bucket

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), materializer, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    // #get-bucket

    final Optional<Bucket> csBucket =
        getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertTrue(csBucket.isPresent());
    assertEquals(Optional.of(this.bucketName()), csBucket.map(Bucket::getName));

    final Optional<Bucket> bucketFromSource =
        getBucketSourceResponse
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertTrue(bucketFromSource.isPresent());
    assertEquals(Optional.of(this.bucketName()), bucketFromSource.map(Bucket::getName));
  }

  @Test
  public void doNotReturnBucketIfBucketDoesNotExist() throws Exception {
    this.mockTokenApi();
    this.mockGetNonExistingBucket();

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), materializer, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    assertEquals(
        Optional.empty(), getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertEquals(
        Optional.empty(),
        getBucketSourceResponse
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenGettingBucketFails() throws Exception {
    this.mockTokenApi();
    this.mockGetBucketFailure();

    final Attributes sampleAttributes = GCStorageAttributes.settings(sampleSettings);

    final CompletionStage<Optional<Bucket>> getBucketResponse =
        GCStorage.getBucket(this.bucketName(), materializer, sampleAttributes);
    final Source<Optional<Bucket>, NotUsed> getBucketSourceResponse =
        GCStorage.getBucketSource(this.bucketName());

    try {
      getBucketResponse.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get bucket failed", e.getMessage());
    }

    try {
      getBucketSourceResponse
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get bucket failed", e.getMessage());
    }
  }

  @Test
  public void listEmptyBucket() throws Exception {
    this.mockTokenApi();
    this.mockEmptyBucketListing();

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertTrue(
        listSource
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listNonExistingFolder() throws Exception {
    final String folder = "folder";
    this.mockTokenApi();
    this.mockNonExistingFolderListing(folder);

    final Source<StorageObject, NotUsed> listSource =
        GCStorage.listBucket(this.bucketName(), folder);

    assertTrue(
        listSource
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listNonExistingBucket() throws Exception {
    this.mockTokenApi();
    ;
    this.mockNonExistingBucketListingJava();

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertTrue(
        listSource
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .isEmpty());
  }

  @Test
  public void listExistingBucketUsingMultipleRequests() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";

    this.mockTokenApi();
    this.mockBucketListingJava(firstFileName, secondFileName);

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(this.bucketName());

    assertEquals(
        Lists.newArrayList(firstFileName, secondFileName),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void listFolderInExistingBucketUsingMultipleRequests() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String folder = "folder";
    final boolean versions = true;

    this.mockTokenApi();
    this.mockBucketListingJava(firstFileName, secondFileName, folder);
    this.mockBucketListingJava(firstFileName, secondFileName, folder, versions);

    // #list-bucket

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(bucketName(), folder);

    final Source<StorageObject, NotUsed> listVersionsSource =
        GCStorage.listBucket(bucketName(), folder, versions);

    // #list-bucket

    assertEquals(
        Lists.newArrayList(firstFileName, secondFileName),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));

    assertEquals(
        Lists.newArrayList(firstFileName, firstFileName + '#' + generation(), secondFileName),
        listVersionsSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenBucketListingFails() throws Exception {

    this.mockTokenApi();
    this.mockBucketListingFailure();

    final Source<StorageObject, NotUsed> listSource = GCStorage.listBucket(bucketName());

    try {
      listSource
          .map(StorageObject::name)
          .runWith(Sink.seq(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Bucket listing failed", e.getMessage());
    }
  }

  @Test
  public void returnEmptySourceWhenListingBucketWithWrongSettings() throws Exception {

    this.mockTokenApi();
    this.mockBucketListingFailure();

    // #list-bucket-attributes

    final GCStorageSettings newBasePathSettings =
        GCStorageExt.get(this.system()).settings().withBasePath("/storage/v1");

    final Source<StorageObject, NotUsed> listSource =
        GCStorage.listBucket(bucketName())
            .withAttributes(GCStorageAttributes.settings(newBasePathSettings));

    // #list-bucket-attributes

    assertEquals(
        Lists.newArrayList(),
        listSource
            .map(StorageObject::name)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void getExistingStorageObject() throws Exception {
    this.mockTokenApi();
    this.mockGetExistingStorageObjectJava();
    this.mockGetExistingStorageObjectJava(generation());

    // #objectMetadata

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    final Source<Optional<StorageObject>, NotUsed> getObjectGenerationSource =
        GCStorage.getObject(bucketName(), fileName(), generation());

    // #objectMetadata

    final Optional<StorageObject> storageObjectOpt =
        getObjectSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertTrue(storageObjectOpt.isPresent());

    final StorageObject storageObject = storageObjectOpt.get();
    assertEquals(fileName(), storageObject.name());
    assertEquals(bucketName(), storageObject.bucket());

    final Optional<StorageObject> storageObjectGenerationOpt =
        getObjectGenerationSource
            .runWith(Sink.head(), materializer)
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
    this.mockTokenApi();
    this.mockGetNonExistingStorageObject();

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    assertEquals(
        Optional.empty(),
        getObjectSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenGetStorageObjectFails() throws Exception {
    this.mockTokenApi();
    this.mockGetNonStorageObjectFailure();

    final Source<Optional<StorageObject>, NotUsed> getObjectSource =
        GCStorage.getObject(bucketName(), fileName());

    try {
      getObjectSource
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Get storage object failed", e.getMessage());
    }
  }

  @Test
  public void downloadFileWhenFileExists() throws Exception {
    final String fileContent = "Google storage file content";
    final String fileContentGeneration = "Google storage file content (archived)";

    this.mockTokenApi();
    this.mockFileDownloadJava(fileContent);
    this.mockFileDownloadJava(fileContentGeneration, generation());

    // #download

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadGenerationSource =
        GCStorage.download(bucketName(), fileName(), generation());

    // #download

    final Source<ByteString, NotUsed> data =
        downloadSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final Source<ByteString, NotUsed> dataGeneration =
        downloadGenerationSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final CompletionStage<List<String>> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.seq(), materializer);

    final CompletionStage<List<String>> resultGenerationCompletionStage =
        dataGeneration.map(ByteString::utf8String).runWith(Sink.seq(), materializer);

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
    this.mockTokenApi();
    this.mockNonExistingFileDownload();

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    assertEquals(
        Optional.empty(),
        downloadSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWithErrorWhenFileDownloadFails() throws Exception {

    this.mockTokenApi();
    this.mockFileDownloadFailure();

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    try {
      downloadSource
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] File download failed", e.getMessage());
    }
  }

  @Test
  public void retryWhenFileDownloadFailsWithServerError() throws Exception {

    final String fileContent = "SomeFileContent";

    this.mockTokenApi();
    this.mockFileDownloadFailureThenSuccess(500, "Internal server error", fileContent);

    final Source<Optional<Source<ByteString, NotUsed>>, NotUsed> downloadSource =
        GCStorage.download(bucketName(), fileName());

    Source<ByteString, NotUsed> data =
        downloadSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(500, TimeUnit.SECONDS)
            .get();

    final CompletionStage<List<String>> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.seq(), materializer);

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

    this.mockTokenApi();
    this.mockUploadSmallFile(fileContent);

    final Source<StorageObject, NotUsed> simpleUploadSource =
        GCStorage.simpleUpload(bucketName(), fileName(), fileSource, contentType);

    final StorageObject storageObject =
        simpleUploadSource
            .runWith(Sink.head(), materializer)
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

    this.mockTokenApi();
    this.mockUploadSmallFileFailure(fileContent);

    final Source<StorageObject, NotUsed> simpleUploadSource =
        GCStorage.simpleUpload(bucketName(), fileName(), fileSource, contentType);

    try {
      simpleUploadSource
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Upload small file failed", e.getMessage());
    }
  }

  @Test
  public void deleteExistingObject() throws Exception {
    this.mockTokenApi();
    this.mockDeleteObjectJava(fileName());
    this.mockDeleteObjectJava(fileName(), generation());

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());
    final Source<Boolean, NotUsed> deleteGenerationSource =
        GCStorage.deleteObject(bucketName(), fileName(), generation());

    assertTrue(
        deleteSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));

    assertTrue(
        deleteGenerationSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void doNotDeleteNonExistingObject() throws Exception {
    this.mockTokenApi();
    this.mockNonExistingDeleteObject(fileName());

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());

    assertFalse(
        deleteSource
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWhenDeleteObjectFails() throws Exception {
    this.mockTokenApi();
    this.mockDeleteObjectFailure(fileName());

    final Source<Boolean, NotUsed> deleteSource = GCStorage.deleteObject(bucketName(), fileName());

    try {
      deleteSource
          .runWith(Sink.head(), materializer)
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Delete object failed", e.getMessage());
    }
  }

  @Test
  public void deleteExistingFolder() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String prefix = "folder";

    this.mockTokenApi();
    this.mockBucketListingJava(firstFileName, secondFileName, prefix);
    this.mockDeleteObjectJava(firstFileName);
    this.mockDeleteObjectJava(secondFileName);

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    assertEquals(
        Lists.newArrayList(true, true),
        deleteObjectsByPrefixSource
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void doNotDeleteNonExistingFolder() throws Exception {
    final String prefix = "folder";

    this.mockTokenApi();
    this.mockNonExistingBucketListingJava(prefix);
    this.mockObjectDoesNotExist(prefix);

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    assertEquals(
        Lists.newArrayList(),
        deleteObjectsByPrefixSource
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void failWhenFolderDeleteFails() throws Exception {
    final String firstFileName = "file1.txt";
    final String secondFileName = "file2.txt";
    final String prefix = "folder";

    this.mockTokenApi();
    this.mockNonExistingBucketListingJava(prefix);
    this.mockBucketListingJava(firstFileName, secondFileName, prefix);
    this.mockDeleteObjectJava(firstFileName);
    this.mockDeleteObjectJava(secondFileName);
    this.mockDeleteObjectFailure(secondFileName);

    final Source<Boolean, NotUsed> deleteObjectsByPrefixSource =
        GCStorage.deleteObjectsByPrefix(bucketName(), prefix);

    try {
      deleteObjectsByPrefixSource
          .runWith(Sink.seq(), materializer)
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

    this.mockTokenApi();
    this.mockLargeFileUpload(firstChunkContent, secondChunkContent, chunkSize);

    // #upload

    final Sink<ByteString, CompletionStage<StorageObject>> sink =
        GCStorage.resumableUpload(
            bucketName(), fileName(), ContentTypes.TEXT_PLAIN_UTF8, chunkSize);

    final Source<ByteString, NotUsed> source =
        Source.from(
            Lists.newArrayList(
                ByteString.fromString(firstChunkContent),
                ByteString.fromString(secondChunkContent)));

    final CompletionStage<StorageObject> result = source.runWith(sink, materializer);

    // #upload

    final StorageObject storageObject = result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(fileName(), storageObject.name());
    assertEquals(bucketName(), storageObject.bucket());
  }

  @Test
  public void failWithErrorWhenLargeFileUploadFails() throws Exception {
    final int chunkSize = 256 * 1024;
    final String firstChunkContent = this.getRandomString(chunkSize);
    final String secondChunkContent = this.getRandomString(chunkSize);

    this.mockTokenApi();
    this.mockLargeFileUploadFailure(firstChunkContent, secondChunkContent, chunkSize);

    final Sink<ByteString, CompletionStage<StorageObject>> sink =
        GCStorage.resumableUpload(
            bucketName(), fileName(), ContentTypes.TEXT_PLAIN_UTF8, chunkSize);

    final Source<ByteString, NotUsed> source =
        Source.from(
            Lists.newArrayList(
                ByteString.fromString(firstChunkContent),
                ByteString.fromString(secondChunkContent)));

    try {
      source.runWith(sink, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      assertEquals(
          "akka.stream.alpakka.googlecloud.storage.FailedUpload: Uploading part failed with status 400 Bad Request: Chunk upload failed",
          e.getMessage());
    }
  }

  @Test
  public void rewriteFile() throws Exception {
    final String rewriteBucketName = "alpakka-rewrite";

    this.mockTokenApi();
    this.mockRewrite(rewriteBucketName);

    // #rewrite

    final CompletionStage<StorageObject> result =
        GCStorage.rewrite(bucketName(), fileName(), rewriteBucketName, fileName())
            .run(materializer);

    // #rewrite

    final StorageObject storageObject = result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(fileName(), storageObject.name());
    assertEquals(rewriteBucketName, storageObject.bucket());
  }

  @Test
  public void failWhenRewriteFileFails() throws Exception {
    final String rewriteBucketName = "alpakka-rewrite";

    this.mockTokenApi();
    this.mockRewriteFailure(rewriteBucketName);

    final CompletionStage<StorageObject> result =
        GCStorage.rewrite(bucketName(), fileName(), rewriteBucketName, fileName())
            .run(materializer);

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);

    } catch (Exception e) {
      assertEquals("java.lang.RuntimeException: [400] Rewrite failed", e.getMessage());
    }
  }
}
