package akka.stream.alpakka.marshal.http;

import static javaslang.control.Option.none;
import static javaslang.control.Option.some;

import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.NotUsed;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethod;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.model.Uri;
import akka.stream.Materializer;
import akka.stream.javadsl.AsPublisher;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import javaslang.control.Option;

public class HttpFlow {
    private static final Logger log = LoggerFactory.getLogger(HttpFlow.class);
    private static final Predicate<HttpResponse> defaultIsSuccess = r -> r.status().isSuccess();
    
    private final Http http;
    private final Materializer materializer;
    
    public HttpFlow(Http http, Materializer materializer) {
        this.http = http;
        this.materializer = materializer;
    }
    
    
    /**
     * Returns a flow that, when materialized, makes an HTTP request with the given [method] to [uri], with an entity
     * of [contentType] and optional extra [headers], using flow input as request body.
     * 
     * The flow output will be the body of the HTTP response.
     * 
     * The materialized value of the flow is the HTTP response itself, which can be used to check headers and status code. It is NOT
     * legal to access the entity data from the materialized value, since its contents is already consumed as the flow output.
     * 
     * @param isSuccess predicate that decides whether a response is acceptable. If not, the stream will be failed with
     *        an IllegalStateException.
     */
    public Flow<ByteString, ByteString, CompletionStage<HttpResponse>> flow(HttpMethod method, Uri uri, ContentType contentType, Predicate<HttpResponse> isSuccess, HttpHeader... headers) {
        return createFlow(method, uri, some(contentType), isSuccess, headers);
    }
    
    public Flow<ByteString, ByteString, CompletionStage<HttpResponse>> flow(HttpMethod method, Uri uri, ContentType contentType, HttpHeader... headers) {
        return flow(method, uri, contentType, defaultIsSuccess, headers);
    }
    
    /**
     * Returns a source that, when materialized, makes an HTTP request with the given [method] to [uri], with no request entity, and optional extra [headers].
     * 
     * The source output will be the body of the HTTP response.
     * 
     * The materialized value of the flow is the HTTP response itself, which can be used to check headers and status code. It is NOT
     * legal to access the entity data from the materialized value, since its contents is already consumed as the flow output.
     * 
     * @param isSuccess predicate that decides whether a response is acceptable. If not, the stream will be failed with
     *        an IllegalStateException.
     */
    public Source<ByteString,CompletionStage<HttpResponse>> source(HttpMethod method, Uri uri, Predicate<HttpResponse> isSuccess, HttpHeader... headers) {
        return Source.<ByteString>empty().viaMat(createFlow(method, uri, none(), isSuccess, headers), (m1,m2) -> m2);
    }
    
    public Source<ByteString,CompletionStage<HttpResponse>> source(HttpMethod method, Uri uri, HttpHeader... headers) {
        return source(method, uri, defaultIsSuccess, headers);
    }

    private Flow<ByteString, ByteString, CompletionStage<HttpResponse>> createFlow(HttpMethod method, Uri uri, Option<ContentType> contentType, Predicate<HttpResponse> isSuccess, HttpHeader... headers) {
        Sink<ByteString, Publisher<ByteString>> in = Sink.asPublisher(AsPublisher.WITH_FANOUT); // akka internally recreates this twice, on some errors...
        Source<ByteString, Subscriber<ByteString>> out = Source.asSubscriber();
        
        return Flow.fromSinkAndSourceMat(in, out, Keep.both()).mapMaterializedValue(pair -> {
            RequestEntity entity;
            if (contentType.isDefined()) {
                Source<ByteString, NotUsed> inReader = Source.fromPublisher(pair.first());
                entity = HttpEntities.createChunked(contentType.get(), inReader);
            } else {
                entity = HttpEntities.EMPTY;
            }
            HttpRequest rq = HttpRequest.create().withMethod(method).withUri(uri).addHeaders(Arrays.asList(headers)).withEntity(entity);
            
            return http.singleRequest(rq, materializer).thenApply(resp -> {
                if (isSuccess.test(resp)) {
                    resp.entity().getDataBytes()
                        .runWith(Sink.fromSubscriber(pair.second()), materializer);
                } else {
                    log.info("Http responded error: {} for request {}", resp, rq);
                    resp.discardEntityBytes(materializer);
                    pair.second().onError(new IllegalStateException("Unsuccessful HTTP response: " + resp + " for " + rq));
                }
                return resp;
            }).exceptionally(x -> {
                Throwable cause = (x instanceof CompletionException) ? x.getCause() : x;
                if (!(cause instanceof IllegalStateException)) {
                    log.info("Could not make http request " + rq, cause);
                }
                pair.second().onError(cause);
                throw (cause instanceof RuntimeException) ? (RuntimeException) x : new RuntimeException(cause);
            });
        });
    }

}
