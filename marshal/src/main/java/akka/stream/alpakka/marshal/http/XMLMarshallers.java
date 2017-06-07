package akka.stream.alpakka.marshal.http;

import java.util.Arrays;

import javax.xml.stream.events.XMLEvent;

import com.tradeshift.reaktive.akka.AsyncMarshallers;
import com.tradeshift.reaktive.akka.AsyncUnmarshallers;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.xml.AaltoReader;
import akka.stream.alpakka.marshal.ProtocolReader;
import akka.stream.alpakka.marshal.ProtocolWriter;
import akka.stream.alpakka.marshal.xml.StaxWriter;

import akka.NotUsed;
import akka.http.javadsl.marshalling.Marshaller;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpCharsets;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.MediaType;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class XMLMarshallers {
    /**
     * Returns a marshaller that will render a stream of T into an XML response using [protocol], with the given [contentType]
     */
    public static <T> Marshaller<Source<T,?>, RequestEntity> sourceToXML(WriteProtocol<XMLEvent, T> protocol, ContentType contentType) {
        return HttpStreamingMarshallers
            .sourceToEntity(contentType)
            .compose((Source<T,?> source) -> source
                .via(ProtocolWriter.of(protocol))
                .via(StaxWriter.flow()));
    }
    
    /**
     * Returns a marshaller that will render a stream of T into an XML response using [protocol], using either text/xml or application/xml.
     */
    public static <T> Marshaller<Source<T,?>, RequestEntity> sourceToXML(WriteProtocol<XMLEvent, T> protocol) {
        return AsyncMarshallers.oneOf(
            sourceToXML(protocol, ContentTypes.TEXT_XML_UTF8),
            sourceToXML(protocol, MediaTypes.APPLICATION_XML.toContentType(HttpCharsets.UTF_8)));
    }
    
 
    /**
     * Returns a marshaller that will render a T into an XML response using [protocol], with the given [contentType].
     */
    public static <T> Marshaller<T, RequestEntity> toXML(WriteProtocol<XMLEvent, T> protocol, ContentType contentType) {
        return sourceToXML(protocol, contentType).compose(Source::single);
    }
    
    /**
     * Returns a marshaller that will render a T into an XML response using [protocol], using either text/xml or application/xml.
     */
    public static <T> Marshaller<T, RequestEntity> toXML(WriteProtocol<XMLEvent, T> protocol) {
        return sourceToXML(protocol).compose(Source::single);
    }
    
    /**
     * Returns an unmarshaller that will read an XML request into a stream of T using [protocol], accepting given media types.
     */
    public static <T> Unmarshaller<HttpEntity, Source<T,NotUsed>> sourceFromXML(ReadProtocol<XMLEvent, T> protocol, MediaType... mediaTypes) {
        return Unmarshaller.forMediaTypes(Arrays.asList(mediaTypes), AsyncUnmarshallers.entityToStream().thenApply(source -> source
            .via(AaltoReader.instance)
            .via(ProtocolReader.of(protocol))));
    }

    /**
     * Returns an unmarshaller that will read an XML request into a stream of T using [protocol], accepting the following media types:
     *      MediaTypes.APPLICATION_ATOM_XML,
     *      MediaTypes.APPLICATION_RSS_XML,
     *      MediaTypes.APPLICATION_SOAP_XML,
     *      MediaTypes.APPLICATION_XML,
     *      MediaTypes.TEXT_XML
     */
    public static <T> Unmarshaller<HttpEntity, Source<T,NotUsed>> sourceFromXML(ReadProtocol<XMLEvent, T> protocol) {
        return sourceFromXML(protocol,
            MediaTypes.APPLICATION_ATOM_XML,
            MediaTypes.APPLICATION_RSS_XML,
            MediaTypes.APPLICATION_SOAP_XML,
            MediaTypes.APPLICATION_XML,
            MediaTypes.TEXT_XML
        );
    }
    
    /**
     * Returns an unmarshaller that will read an XML request into a T using [protocol], accepting the given media types.
     */
    public static <T> Unmarshaller<HttpEntity, T> fromXML(ReadProtocol<XMLEvent, T> protocol, MediaType... mediaTypes) {
        return head(sourceFromXML(protocol, mediaTypes));
    }

    /**
     * Returns an unmarshaller that will read an XML request into a T using [protocol], accepting the following media types:
     *      MediaTypes.APPLICATION_ATOM_XML,
     *      MediaTypes.APPLICATION_RSS_XML,
     *      MediaTypes.APPLICATION_SOAP_XML,
     *      MediaTypes.APPLICATION_XML,
     *      MediaTypes.TEXT_XML
     */
    public static <T> Unmarshaller<HttpEntity, T> fromXML(ReadProtocol<XMLEvent, T> protocol) {
        return head(sourceFromXML(protocol));
    }
    
    private static <T> Unmarshaller<HttpEntity, T> head(Unmarshaller<HttpEntity, Source<T, NotUsed>> streamUnmarshaller) {
        return AsyncUnmarshallers.<HttpEntity, T>withMaterializer((ctx, mat, entity) -> {
            return streamUnmarshaller.unmarshal(entity, ctx, mat).thenCompose(src -> src.runWith(Sink.head(), mat));
        });
    }
}
