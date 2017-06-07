package akka.stream.alpakka.marshal.http;

import static com.tradeshift.reaktive.akka.AsyncUnmarshallers.entityToStream;

import java.util.Arrays;

import com.tradeshift.reaktive.akka.AsyncUnmarshallers;
import akka.stream.alpakka.marshal.json.JSONEvent;
import akka.stream.alpakka.marshal.json.JacksonWriter;
import akka.stream.alpakka.marshal.json.ActsonReader;
import akka.stream.alpakka.marshal.ProtocolReader;
import akka.stream.alpakka.marshal.ProtocolWriter;
import akka.stream.alpakka.marshal.ReadProtocol;

import akka.NotUsed;
import akka.http.javadsl.marshalling.Marshaller;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentType.WithFixedCharset;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.http.javadsl.model.MediaType;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.alpakka.marshal.WriteProtocol;

public class JSONMarshallers {

    /**
     * Returns a marshaller that will render a stream of T into a JSON response using [protocol],
     * with the given content type.
     */
    public static <T> Marshaller<Source<T,?>, RequestEntity> sourceToJSON(WriteProtocol<JSONEvent, T> protocol, ContentType contentType) {
        return HttpStreamingMarshallers
            .sourceToEntity(contentType)
            .compose((Source<T,?> source) -> source
                .via(ProtocolWriter.of(protocol))
                .via(JacksonWriter.flow()));
    }
    
    /**
     * Returns a marshaller that will render a stream of T into a JSON response using [protocol],
     * as application/json.
     */
    public static <T> Marshaller<Source<T,?>, RequestEntity> sourceToJSON(WriteProtocol<JSONEvent, T> protocol) {
        return sourceToJSON(protocol, APPLICATION_JSON);
    }
    
 
    /**
     * Returns a marshaller that will render a T into a JSON response using [protocol],
     * with the given content type
     */
    public static <T> Marshaller<T, RequestEntity> toJSON(WriteProtocol<JSONEvent, T> protocol, ContentType contentType) {
        return sourceToJSON(protocol, contentType).compose((T t) -> Source.single(t));
    }
    
    /**
     * Returns a marshaller that will render a T into a JSON response using [protocol],
     * as application/json
     */
    public static <T> Marshaller<T, RequestEntity> toJSON(WriteProtocol<JSONEvent, T> protocol) {
        return toJSON(protocol, APPLICATION_JSON);
    }
    
    /**
     * Returns an unmarshaller that will read a JSON request into a stream of T using [protocol],
     * if it has any of the given media types.
     */
    public static <T> Unmarshaller<HttpEntity, Source<T,NotUsed>> sourceFromJSON(ReadProtocol<JSONEvent, T> protocol, MediaType... mediaTypes) {
        return Unmarshaller.forMediaTypes(Arrays.asList(mediaTypes), entityToStream().thenApply(source -> source
            .via(ActsonReader.instance)
            .via(ProtocolReader.of(protocol))));
    }
    
    /**
     * Returns an unmarshaller that will read a JSON request into a stream of T using [protocol], accepting the following media types:
     *   MediaTypes.APPLICATION_JSON,
     *   MediaTypes.APPLICATION_JSON_PATCH_JSON,
     *   MediaTypes.APPLICATION_VND_API_JSON
     */
    public static <T> Unmarshaller<HttpEntity, Source<T,NotUsed>> sourceFromJSON(ReadProtocol<JSONEvent, T> protocol) {
        return sourceFromJSON(protocol, ACCEPTED_JSON);
    }
    
    /**
     * Returns an unmarshaller that will read a JSON request into a T using [protocol],
     * if it has any of the given media types.
     */
    public static <T> Unmarshaller<HttpEntity, T> fromJSON(ReadProtocol<JSONEvent, T> protocol, MediaType... mediaTypes) {
        Unmarshaller<HttpEntity, Source<T, NotUsed>> streamUnmarshaller = sourceFromJSON(protocol, mediaTypes);
        
        return AsyncUnmarshallers.<HttpEntity, T>withMaterializer((ctx, mat, entity) -> {
            return streamUnmarshaller.unmarshal(entity, ctx, mat).thenCompose(src -> src.runWith(Sink.head(), mat));
        });
    }
    
    /**
     * Returns an unmarshaller that will read a JSON request into a T using [protocol], accepting the following media types:
     *   MediaTypes.APPLICATION_JSON,
     *   MediaTypes.APPLICATION_JSON_PATCH_JSON,
     *   MediaTypes.APPLICATION_VND_API_JSON
     */
    public static <T> Unmarshaller<HttpEntity, T> fromJSON(ReadProtocol<JSONEvent, T> protocol) {
        return fromJSON(protocol, ACCEPTED_JSON);
    }
    
    private static final WithFixedCharset APPLICATION_JSON = MediaTypes.APPLICATION_JSON.toContentType();

    /** The known JSON media types that are accepted by default*/
    private static final MediaType[] ACCEPTED_JSON = Arrays.asList(
        MediaTypes.APPLICATION_JSON,
        MediaTypes.APPLICATION_JSON_PATCH_JSON,
        MediaTypes.APPLICATION_VND_API_JSON
    ).toArray(new MediaType[0]);
    
}
