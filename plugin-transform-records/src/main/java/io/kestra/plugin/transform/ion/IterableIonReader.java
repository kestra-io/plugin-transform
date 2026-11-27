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

    public static final String NOT_A_STRUCT = "The %s input must contain one Ion struct per row, found %s.";

    private final InputStream inputStream;
    private final IonReader reader;

    // Name of the input in error messages.
    private final String inputName;

    // Rows returned so far: the row count of the input once it has been fully iterated.
    private long rowCount = 0;

    public IterableIonReader(InputStream inputStream, String inputName) {
        this.inputStream = inputStream;
        this.reader = IonValueUtils.system().newReader(inputStream);
        this.inputName = inputName;
    }

    public long getRowCount() {
        return rowCount;
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
                // Unchecked, because Iterator.next() cannot throw a checked exception.
                if (nextType != IonType.STRUCT) {
                    throw new IllegalArgumentException(String.format(NOT_A_STRUCT, inputName, nextType));
                }
                var struct = (IonStruct) IonValueUtils.system().newValue(reader);
                rowCount++;
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
