package io.kestra.plugin.transform.util;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonTextWriterBuilder;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.ion.IterableIonReader;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Reads the Join inputs (in-memory records, or stored Ion files that are streamed) and writes its output
 * (a stored Ion file, or records). Holds open streams: always close it.
 */
public class JoinIOHandler implements AutoCloseable {

    private final RunContext runContext;
    private final Logger logger;
    private final TransformTaskSupport.ResolvedInput leftInput;
    private final TransformTaskSupport.ResolvedInput rightInput;

    // Null when the input is a stored file.
    private final List<IonStruct> leftRecords;
    private final List<IonStruct> rightRecords;

    private final MergeMode mergeMode;
    private final String leftLabel;
    private final String rightLabel;
    private final String prefixSeparator;

    // Null when the output is stored.
    private final List<Object> records;

    // Readers of the stored inputs, that also count their rows.
    private final Map<JoinSide, IterableIonReader> readers = new EnumMap<>(JoinSide.class);

    // Null when the output is not stored.
    private Path outputFile;

    private OutputStream outputStream;
    private IonWriter ionWriter;
    private long outputSize = 0;

    // Null for the default state.
    private State.Type state;

    public JoinIOHandler(
        RunContext runContext,
        TransformTaskSupport.ResolvedInput leftInput,
        TransformTaskSupport.ResolvedInput rightInput,
        boolean storeOutput,
        OutputFormat outputFormat,
        MergeMode mergeMode,
        String leftLabel,
        String rightLabel,
        String prefixSeparator
    ) throws TransformException {

        this.runContext = runContext;
        this.logger = runContext.logger();
        this.leftInput = leftInput;
        this.rightInput = rightInput;
        this.mergeMode = mergeMode;
        this.leftLabel = leftLabel;
        this.rightLabel = rightLabel;
        this.prefixSeparator = prefixSeparator;

        leftRecords = leftInput.fromStorage() ? null : TransformTaskSupport.normalizeRecords(leftInput.value());
        rightRecords = rightInput.fromStorage() ? null : TransformTaskSupport.normalizeRecords(rightInput.value());

        records = storeOutput ? null : new ArrayList<>();

        if (storeOutput) {
            try {
                outputFile = runContext.workingDir().createTempFile(".ion");
                outputStream = TransformTaskSupport.wrapCompression(TransformTaskSupport.bufferedOutput(outputFile));
                ionWriter = createWriter(outputStream, outputFormat);
            } catch (IOException e) {
                var exception = new TransformException(JoinMessages.STORAGE_ERROR, e);
                try {
                    close();
                } catch (IOException closeException) {
                    exception.addSuppressed(closeException);
                }
                throw exception;
            }
        }
    }

    /** A stored input is streamed by a reader closed with this handler. It can only be read once, which gives its row count. */
    public Iterable<IonStruct> getInputReader(JoinSide side) throws TransformException {
        var inputRecords = inputRecords(side);
        if (inputRecords != null) {
            return inputRecords;
        }
        if (readers.containsKey(side)) {
            throw new IllegalStateException("The " + side + " input has already been read.");
        }
        var reader = createInputReader(side);
        readers.put(side, reader);
        return reader;
    }

    /** Complete once the join has read the input. */
    public long getInputCount(JoinSide side) {
        var inputRecords = inputRecords(side);
        if (inputRecords != null) {
            return inputRecords.size();
        }
        var reader = readers.get(side);
        if (reader == null) {
            throw new IllegalStateException("The " + side + " input has not been read.");
        }
        return reader.getRowCount();
    }

    /**
     * Chosen without reading the inputs. An input given as records is already in memory, so it is preferred
     * to a stored one; two stored inputs are compared by file size. LEFT on a tie.
     */
    public JoinSide getSideToHoldInMemory() throws TransformException {
        if (leftRecords != null && rightRecords != null) {
            return leftRecords.size() <= rightRecords.size() ? JoinSide.LEFT : JoinSide.RIGHT;
        }
        if (leftRecords != null) {
            return JoinSide.LEFT;
        }
        if (rightRecords != null) {
            return JoinSide.RIGHT;
        }
        return storedSize(JoinSide.LEFT) <= storedSize(JoinSide.RIGHT) ? JoinSide.LEFT : JoinSide.RIGHT;
    }

    /** Once the join has read both inputs. */
    public void warnEmptyInputs() {
        for (var side : JoinSide.values()) {
            if (getInputCount(side) == 0) {
                warn(String.format(JoinMessages.EMPTY_INPUT, side));
            }
        }
    }

    public long getOutputSize() {
        return outputSize;
    }

    public List<Object> getRecords() {
        return records;
    }

    public State.Type getState() {
        return state;
    }

    public void markWarning() {
        this.state = State.Type.WARNING;
    }

    public void warn(String message) {
        logger.warn(message);
    }

    public void writeMergedRow(IonStruct leftRow, IonStruct rightRow) throws TransformException {
        try {
            write(buildRow(leftRow, rightRow));
        } catch (IOException | RuntimeException e) {
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR, "a merged row"), e);
        }
    }

    public void writeSideRow(IonStruct row, JoinSide side) throws TransformException {
        try {
            write(side == JoinSide.LEFT ? buildRow(row, null) : buildRow(null, row));
        } catch (IOException | RuntimeException e) {
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR, "a row from the " + side + " input"), e);
        }
    }

    /** Returns null when the output is not stored. */
    public URI storeOutputFile() throws TransformException {
        if (outputFile == null) {
            return null;
        }
        try {
            closeWriter();
            return runContext.storage().putFile(outputFile.toFile(), "join-" + UUID.randomUUID() + ".ion");
        } catch (IOException e) {
            throw new TransformException(JoinMessages.STORAGE_ERROR, e);
        }
    }

    /** Safe to call more than once. */
    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            closeWriter();
        } catch (IOException e) {
            failure = e;
        }
        for (var reader : readers.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        readers.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private IonStruct buildRow(IonStruct leftRow, IonStruct rightRow) {
        // Left first, then right: on a field name conflict, the right value replaces the left one
        // and the field keeps its first position (IonStruct.put would move it to the end).
        var fields = new LinkedHashMap<String, IonValue>();
        addFields(fields, leftRow, leftLabel);
        addFields(fields, rightRow, rightLabel);

        var merged = IonValueUtils.system().newEmptyStruct();
        for (var field : fields.entrySet()) {
            // Values are cloned because a value cannot be a member of multiple structs.
            merged.add(field.getKey(), IonValueUtils.cloneValue(field.getValue()));
        }
        return merged;
    }

    private void addFields(Map<String, IonValue> fields, IonStruct row, String label) {
        if (row == null) {
            return;
        }
        switch (mergeMode) {
            case NEST -> fields.put(label, row);
            case PREFIX -> {
                for (var field : row) {
                    fields.put(label + prefixSeparator + field.getFieldName(), field);
                }
            }
            case OVERWRITE -> {
                for (var field : row) {
                    fields.put(field.getFieldName(), field);
                }
            }
        }
    }

    /** Text is written in UTF-8, unlike the default ASCII writer of the Ion system, which escapes accents. */
    private static IonWriter createWriter(OutputStream outputStream, OutputFormat format) throws IOException {
        if (format == OutputFormat.TEXT) {
            return IonTextWriterBuilder.standard()
                .withCharset(IonTextWriterBuilder.UTF8)
                .withWriteTopLevelValuesOnNewLines(true)
                .withNewLineType(IonTextWriterBuilder.NewLineType.LF)
                .build(outputStream);
        }
        return TransformTaskSupport.createWriter(outputStream, format);
    }

    private void write(IonStruct row) throws IOException {
        if (records != null) {
            records.add(IonValueUtils.toJavaValue(row));
        } else {
            row.writeTo(ionWriter);
        }
        outputSize++;
    }

    private void closeWriter() throws IOException {
        try {
            if (ionWriter != null) ionWriter.close();
        } finally {
            ionWriter = null;
            if (outputStream != null) {
                var stream = outputStream;
                outputStream = null;
                stream.close();
            }
        }
    }

    // Null when the input is a stored file.
    private List<IonStruct> inputRecords(JoinSide side) {
        return side == JoinSide.LEFT ? leftRecords : rightRecords;
    }

    private URI storageUri(JoinSide side) {
        return (side == JoinSide.LEFT ? leftInput : rightInput).storageUri();
    }

    private long storedSize(JoinSide side) throws TransformException {
        var inputUri = storageUri(side);
        try {
            return runContext.storage().getAttributes(inputUri).getSize();
        } catch (IOException e) {
            throw new TransformException(String.format(JoinMessages.READ_ERROR, inputUri), e);
        }
    }

    /** The reader must be closed by the caller. */
    private IterableIonReader createInputReader(JoinSide side) throws TransformException {
        var inputUri = storageUri(side);
        InputStream inputStream = null;
        try {
            inputStream = runContext.storage().getFile(inputUri);
            return new IterableIonReader(inputStream, side.toString());
        } catch (IOException | RuntimeException e) {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw new TransformException(String.format(JoinMessages.READ_ERROR, inputUri), e);
        }
    }
}
