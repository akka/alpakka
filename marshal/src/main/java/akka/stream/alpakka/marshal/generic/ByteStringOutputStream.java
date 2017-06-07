package akka.stream.alpakka.marshal.generic;

import java.io.IOException;
import java.io.OutputStream;

import akka.util.ByteString;
import akka.util.ByteStringBuilder;

/**
 * Captures bytes written to an output stream as ByteString instances, which can be picked off
 * repeatedly by invoking {@link #getBytesAndReset()}.
 * 
 * Instances of this class are not safe to be invoked concurrently from multiple threads.
 */
public class ByteStringOutputStream extends OutputStream {
    private ByteStringBuilder buffer = new ByteStringBuilder();
    private OutputStream delegate = buffer.asOutputStream();

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Returns a ByteString containing the bytes that have been written so far, and then
     * empties the internal buffer.
     */
    public ByteString getBytesAndReset() {
        try {
            delegate.close();
        } catch (IOException e) { // can't occur, we're not doing I/O
            throw new RuntimeException(e);
        }
        ByteString result = buffer.result();
        buffer = new ByteStringBuilder();
        delegate = buffer.asOutputStream();
        return result;
    }

    public boolean hasBytes() {
        return buffer.length() > 0;
    }
    
}