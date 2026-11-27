package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Streams the top-level structs of an Ion input. It can only be iterated once (the reader is consumed),
 * and closing it also closes the input stream.
 */
public class IterableIonReader implements Iterable<IonStruct>, AutoCloseable {

    private final InputStream inputStream;
    private final IonReader reader;

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
                var struct = (IonStruct) IonValueUtils.system().newValue(reader);
                nextType = reader.next();
                return struct;
            }
        };
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } finally {
            inputStream.close();
        }
    }
}
