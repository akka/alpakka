package akka.stream.alpakka.marshal.csv;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
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
 * Encodes a stream of strings into a stream of bytes, using a supplied {@link Charset}.
 */
public class CharsetEncoderFlow extends GraphStage<FlowShape<String, ByteString>> {
    private final Inlet<String> in = Inlet.create("in");
    private final Outlet<ByteString> out = Outlet.create("out");
    private final FlowShape<String, ByteString> shape = FlowShape.of(in, out);
    private final Charset encoding;
    
    public CharsetEncoderFlow(Charset encoding) {
        this.encoding = encoding;
    }

    @Override
    public FlowShape<String, ByteString> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes arg0) throws Exception {
        return new GraphStageLogic(shape) {
            final CharsetEncoder encoder = encoding.newEncoder();
            String buf = "";
            {
                setHandlers(in, out, new AbstractInOutHandler() {
                    @Override
                    public void onPull() {
                        pull(in);
                    }
                    
                    @Override
                    public void onPush() throws CharacterCodingException {
                        String input = grab(in);
                        CharBuffer chars = CharBuffer.allocate(input.length() + buf.length());
                        chars.append(buf);
                        chars.append(input);
                        chars.rewind();
                        deliver(chars);
                    }

                    @Override
                    public void onUpstreamFinish() throws CharacterCodingException {
                        deliver(CharBuffer.wrap(buf));
                        if (!buf.isEmpty()) {
                            throw new IllegalArgumentException("Stray bytes at end of input that could not be decoded: " + buf);
                        }
                        completeStage();
                    }
                                        
                    private void deliver(CharBuffer chars) throws CharacterCodingException {
                        ByteBuffer bytes = ByteBuffer.allocate(chars.limit() * 4);
                        CoderResult result = encoder.encode(chars, bytes, false);
                        if (result.isOverflow()) {
                            failStage(new IllegalArgumentException("Incoming chars decoded into more than size*4 characters. Huh?"));
                            return;
                        }
                        if (result.isError()) {
                            result.throwException();
                        }
                        
                        int count = bytes.position();
                        bytes.rewind();
                        bytes.limit(count);
                        if (count > 0) {
                            push(out, ByteString.fromByteBuffer(bytes));
                        } else if (!isClosed(in)) {
                            pull(in);
                        }
                        
                        // save the remaining chars for later
                        buf = chars.toString();
                    }
                });
            }
        };
    }
}
