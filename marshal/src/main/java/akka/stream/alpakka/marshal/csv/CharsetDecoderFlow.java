package akka.stream.alpakka.marshal.csv;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;

/**
 * Decodes a stream of bytes into a stream of characters, using a supplied {@link Charset}.
 */
public class CharsetDecoderFlow extends GraphStage<FlowShape<ByteString, String>> {

    private final Inlet<ByteString> in = Inlet.create("in");
    private final Outlet<String> out = Outlet.create("out");
    private final FlowShape<ByteString, String> shape = FlowShape.of(in, out);
    
    private final Charset encoding;
    
    public CharsetDecoderFlow(Charset encoding) {
        this.encoding = encoding;
    }

    @Override
    public FlowShape<ByteString, String> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes arg0) throws Exception {
        return new GraphStageLogic(shape) {
            final CharsetDecoder decoder = encoding.newDecoder();
            ByteString buf = ByteString.empty();
            {
                setHandlers(in, out, new AbstractInOutHandler() {
                    @Override
                    public void onPull() {
                        pull(in);
                    }
                    
                    @Override
                    public void onPush() throws CharacterCodingException {
                        deliver(buf.concat(grab(in)).asByteBuffer());
                    }

                    @Override
                    public void onUpstreamFinish() throws CharacterCodingException {
                        deliver(buf.asByteBuffer());
                        if (!buf.isEmpty()) {
                            throw new IllegalArgumentException("Stray bytes at end of input that could not be decoded: " + buf);
                        }
                        completeStage();
                    }
                    
                    private void deliver(ByteBuffer bytes) throws CharacterCodingException {
                        CharBuffer chars = CharBuffer.allocate(bytes.limit());
                        CoderResult result = decoder.decode(bytes, chars, false);
                        if (result.isOverflow()) {
                            failStage(new IllegalArgumentException("Incoming bytes decoded into more characters. Huh?"));
                            return;
                        }
                        if (result.isError()) {
                            result.throwException();
                        }
                        
                        int count = chars.position();
                        chars.rewind();
                        if (count > 0) {
                            push(out, new String(chars.array(), chars.position(), count));
                        } else if (!isClosed(in)) {
                            pull(in);
                        }
                        
                        // save the remaining bytes for later
                        buf = ByteString.fromByteBuffer(bytes);
                    }
                });
            }
        };
    }

}
