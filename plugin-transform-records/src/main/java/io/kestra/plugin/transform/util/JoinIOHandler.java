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

    private final long leftSize;
    private final long rightSize;
    private final MergeMode mergeMode;
    private final String leftLabel;
    private final String rightLabel;
    private final String prefixSeparator;

    // Null when the output is stored.
    private final List<Object> records;

    private final List<IterableIonReader> openReaders = new ArrayList<>();

    // Null when the output is not stored.
    private Path outputFile;

    private OutputStream outputStream;
    private IonWriter ionWriter;
    private URI outputUri;
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

        leftSize = countInputRows(JoinSide.LEFT);
        rightSize = countInputRows(JoinSide.RIGHT);

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

    /** A stored input is streamed by a new reader at each call, closed with this handler. */
    public Iterable<IonStruct> getInputReader(JoinSide side) throws TransformException {
        var inputRecords = side == JoinSide.LEFT ? leftRecords : rightRecords;
        if (inputRecords != null) {
            return inputRecords;
        }
        var reader = createInputReader(storageUri(side));
        openReaders.add(reader);
        return reader;
    }

    public long getInputCount(JoinSide side) {
        return side == JoinSide.LEFT ? leftSize : rightSize;
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
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR,
                String.format("a merged row between %s and %s", leftRow, rightRow)
            ), e);
        }
    }

    public void writeSideRow(IonStruct row, JoinSide side) throws TransformException {
        try {
            write(side == JoinSide.LEFT ? buildRow(row, null) : buildRow(null, row));
        } catch (IOException | RuntimeException e) {
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR,
                String.format("%s from %s side", row, side)
            ), e);
        }
    }

    /** Returns null when the output is not stored. */
    public URI storeOutputFile() throws TransformException {
        if (outputFile == null) {
            return null;
        }
        try {
            closeWriter();
            outputUri = runContext.storage().putFile(outputFile.toFile(), "join-" + UUID.randomUUID() + ".ion");
            return outputUri;
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
        for (var reader : openReaders) {
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
        openReaders.clear();
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

    private URI storageUri(JoinSide side) {
        return (side == JoinSide.LEFT ? leftInput : rightInput).storageUri();
    }

    /** The reader must be closed by the caller. */
    private IterableIonReader createInputReader(URI inputUri) throws TransformException {
        InputStream inputStream = null;
        try {
            inputStream = runContext.storage().getFile(inputUri);
            return new IterableIonReader(inputStream);
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

    private long countInputRows(JoinSide side) throws TransformException {
        var inputRecords = side == JoinSide.LEFT ? leftRecords : rightRecords;
        var count = 0L;

        if (inputRecords != null) {
            count = inputRecords.size();
        } else {
            var inputUri = storageUri(side);
            try (var reader = createInputReader(inputUri)) {
                for (var ignored : reader) {
                    count++;
                }
            } catch (IOException | RuntimeException e) {
                throw new TransformException(String.format(JoinMessages.READ_ERROR, inputUri), e);
            }
        }

        if (count < 1) warn(String.format(JoinMessages.EMPTY_INPUT, side));

        return count;
    }
}
