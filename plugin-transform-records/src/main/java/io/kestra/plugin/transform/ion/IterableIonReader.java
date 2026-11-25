package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wrapper of IonReader that makes it iterable, streaming top-level structs one by one.
 * The underlying reader is consumed by iteration, so an instance can only be iterated once.
 * Closing it closes both the reader and the input stream.
 */
public class IterableIonReader implements Iterable<IonStruct>, AutoCloseable {

    /** Data stream of input. */
    private final InputStream inputStream;

    /** ION reader instance. */
    private final IonReader reader;

    /**
     * Creates a new IterableIonReader.
     * @param inputStream Data stream of input, closed with this reader.
     */
    public IterableIonReader(InputStream inputStream) {
        this.inputStream = inputStream;
        this.reader = IonValueUtils.system().newReader(inputStream);
    }

    @Override
    public Iterator<IonStruct> iterator() {
        return new Iterator<>() {
            IonType nextType = reader.next();

            @Override
            public boolean hasNext() {
                return nextType != null;
            }

            @Override
            public IonStruct next() {
                if (nextType == null) {
                    throw new NoSuchElementException();
                }
                IonStruct struct = (IonStruct) IonValueUtils.system().newValue(reader);
                nextType = reader.next();
                return struct;
            }
        };
    }

    /**
     * Closes the ION reader and the input stream.
     * @throws IOException If one of them cannot be closed.
     */
    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } finally {
            inputStream.close();
        }
    }
}
