package akka.stream.alpakka.marshal.generic;

import java.nio.charset.StandardCharsets;

import akka.NotUsed;
import akka.parboiled2.util.Base64;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.javadsl.Flow;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import scala.Tuple2;

public abstract class Base64Decoder {
    /**
     * A graph stage that decodes input bytes, which are assumed to be ASCII base64-encoded, into their binary representation.
     * 
     * The implementation is according to RFC4648.
     */
    public static final Flow<ByteString,ByteString,NotUsed> decodeBase64Bytes = Flow.fromGraph(new GraphStage<FlowShape<ByteString,ByteString>>() {
        private final Inlet<ByteString> in = Inlet.create("in");
        private final Outlet<ByteString> out = Outlet.create("out");
        private final FlowShape<ByteString, ByteString> shape = FlowShape.of(in, out);

        @Override
        public FlowShape<ByteString, ByteString> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes attr) throws Exception {
            return new GraphStageLogic(shape) {
                private ByteString buf = ByteString.empty();
                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() {
                            buf = buf.concat(grab(in));
                            // only decode in 4-character groups
                            Tuple2<ByteString, ByteString> split = buf.splitAt(buf.size() - (buf.size() % 4));
                            if (split._1.isEmpty()) {
                                pull(in);
                            } else {
                                try {
                                    push(out, decode(split._1));
                                } catch (IllegalArgumentException x) {
                                    failStage(x);
                                }
                            }
                            buf = split._2;
                        }

                        private ByteString decode(ByteString encoded) {
                            byte[] a = Base64.rfc2045().decode(encoded.toArray());
                            if (a == null) {
                                throw new IllegalArgumentException("Base64 input is not a valid multiple of 4-char sequences.");
                            }
                            return unsafeWrapByteArray(a);
                        }
                        
                        public void onUpstreamFinish() {
                            if (buf.isEmpty()) {
                                complete(out);
                            } else {
                                try {
                                    emit(out, decode(buf), () -> complete(out));
                                } catch (IllegalArgumentException x) {
                                    failStage(x);
                                }
                            }
                        };
                    });
                    
                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() {
                            pull(in);
                        }
                    });
                }
            };
        }
    });
    
    /**
     * A graph stage that decodes input strings, which are assumed to be ASCII base64-encoded, into their binary representation.
     */
    public static final Flow<String,ByteString,NotUsed> decodeBase64Strings =
        Flow.of(String.class)
            .map(s -> s.getBytes(StandardCharsets.ISO_8859_1))
            .map(Base64Decoder::unsafeWrapByteArray)
            .via(decodeBase64Bytes);
        
    /**
     * Wraps a byte array that is known to no longer be externally modified by any thread, ever, in an immutable ByteString.
     * 
     * This uses internal akka API, which is unsupported (but saves an array copy operation).
     */
    private static ByteString unsafeWrapByteArray(byte[] bytes) {
        return new ByteStringBuilder()
            .putByteArrayUnsafe(bytes)
            .result();
    }
}
