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
 * Layer to handle I/O operations of the Join task with Kestra run context and ION formatting.
 * <p>
 * Inputs are either in-memory records or Ion files from the internal storage; files are streamed, never loaded at once.
 * Output rows are either written to a temporary Ion file (then stored) or collected as records.
 * Holds open streams: always close it, e.g. with try-with-resources.
 */
public class JoinIOHandler implements AutoCloseable {

    /** Kestra run context. */
    private final RunContext runContext;

    /** The Kestra logger (slf4j) for the task. */
    private final Logger logger;

    /** Resolved left input. */
    private final TransformTaskSupport.ResolvedInput leftInput;

    /** Resolved right input. */
    private final TransformTaskSupport.ResolvedInput rightInput;

    /** Left records when the left input is in memory, null when it is a stored file. */
    private final List<IonStruct> leftRecords;

    /** Right records when the right input is in memory, null when it is a stored file. */
    private final List<IonStruct> rightRecords;

    /** Count of left rows. */
    private final long leftSize;

    /** Count of right rows. */
    private final long rightSize;

    /** Merge mode of output fields. */
    private final MergeMode mergeMode;

    /** Label for left fields in output. */
    private final String leftLabel;

    /** Label for right fields in output. */
    private final String rightLabel;

    /** Separator character for prefixes. */
    private final String prefixSeparator;

    /** Output records when the output is not stored, null otherwise. */
    private final List<Object> records;

    /** Readers opened on stored inputs, closed with this handler. */
    private final List<IterableIonReader> openReaders = new ArrayList<>();

    /** Temporary output file in the task working directory, null when the output is not stored. */
    private Path outputFile;

    /** Data stream of output file. */
    private OutputStream outputStream;

    /** ION writer for output file. */
    private IonWriter ionWriter;

    /** URI of stored output file. */
    private URI outputUri;

    /** Count of output rows. */
    private long outputSize = 0;

    /** Final state of the task, null for the default state. */
    private State.Type state;

    /**
     * Creates a new JoinIOHandler.
     * Counts the rows of both inputs and opens the output; everything already opened is closed if a later step fails.
     * @param runContext Kestra run context.
     * @param leftInput Resolved left input.
     * @param rightInput Resolved right input.
     * @param storeOutput True to store output rows in a file, false to collect them as records.
     * @param outputFormat Output format of the stored file.
     * @param mergeMode Merge mode of output fields.
     * @param leftLabel Label for left fields in output.
     * @param rightLabel Label for right fields in output.
     * @param prefixSeparator Separator character for prefixes.
     * @throws TransformException If an input cannot be read or the output cannot be opened.
     */
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

    /**
     * Get the rows of an input.
     * A stored input is streamed by a new reader at each call, closed with this handler.
     * @param side Side of input.
     * @return Rows of input.
     * @throws TransformException If the stored input cannot be opened.
     */
    public Iterable<IonStruct> getInputReader(JoinSide side) throws TransformException {
        var inputRecords = side == JoinSide.LEFT ? leftRecords : rightRecords;
        if (inputRecords != null) {
            return inputRecords;
        }
        var reader = createInputReader(storageUri(side));
        openReaders.add(reader);
        return reader;
    }

    /**
     * Get the row count of an input.
     * @param side Side of input.
     * @return count.
     */
    public long getInputCount(JoinSide side) {
        return side == JoinSide.LEFT ? leftSize : rightSize;
    }

    /**
     * Get the row count of output.
     * @return count.
     */
    public long getOutputSize() {
        return outputSize;
    }

    /**
     * Get output records.
     * @return Output records, or null when the output is stored.
     */
    public List<Object> getRecords() {
        return records;
    }

    /**
     * Get final state of task.
     * @return WARNING, or null for the default state.
     */
    public State.Type getState() {
        return state;
    }

    /**
     * Sets the final state of task to WARNING.
     */
    public void markWarning() {
        this.state = State.Type.WARNING;
    }

    /**
     * Log a message at a WARN level.
     * @param message Message to log.
     */
    public void warn(String message) {
        logger.warn(message);
    }

    /**
     * Writes a row merging left and right data.
     * @param leftRow Row (IonStruct) from left input.
     * @param rightRow Row (IonStruct) from right input.
     * @throws TransformException If the row cannot be written.
     */
    public void writeMergedRow(IonStruct leftRow, IonStruct rightRow) throws TransformException {
        try {
            write(buildRow(leftRow, rightRow));
        } catch (IOException | RuntimeException e) {
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR,
                String.format("a merged row between %s and %s", leftRow, rightRow)
            ), e);
        }
    }

    /**
     * Writes a row from a single input (non-matched or semi join entry).
     * @param row Row (IonStruct) from the input.
     * @param side Side of the input (left or right).
     * @throws TransformException If the row cannot be written.
     */
    public void writeSideRow(IonStruct row, JoinSide side) throws TransformException {
        try {
            write(side == JoinSide.LEFT ? buildRow(row, null) : buildRow(null, row));
        } catch (IOException | RuntimeException e) {
            throw new TransformException(String.format(JoinMessages.WRITE_ERROR,
                String.format("%s from %s side", row, side)
            ), e);
        }
    }

    /**
     * Completes the output file and stores it in the Kestra storage.
     * @return URI of the output file, or null when the output is not stored.
     * @throws TransformException If the file cannot be completed or stored.
     */
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

    /**
     * Closes the inputs and the output file. Safe to call more than once.
     * @throws IOException If a stream cannot be closed.
     */
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

    /**
     * Builds an output row from one or both sides, according to the merge mode.
     * @param leftRow Row from left input, or null.
     * @param rightRow Row from right input, or null.
     * @return The output row.
     */
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

    /**
     * Collects the fields of a row for an output row, according to the merge mode.
     * @param fields Output fields, by name.
     * @param row Row from an input, or null.
     * @param label Label of the input side.
     */
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

    /**
     * Creates the ION writer of the output file.
     * Text is written in UTF-8 (unlike the default ASCII writer of the Ion system, which escapes accents),
     * one row per line, by the writer itself.
     * @param outputStream Data stream of output file.
     * @param format Output format.
     * @return The ION writer.
     * @throws IOException If the writer cannot be created.
     */
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

    /**
     * Writes an output row to the output file or to the output records.
     * @param row Output row.
     * @throws IOException If the row cannot be written.
     */
    private void write(IonStruct row) throws IOException {
        if (records != null) {
            records.add(IonValueUtils.toJavaValue(row));
        } else {
            row.writeTo(ionWriter);
        }
        outputSize++;
    }

    /**
     * Flushes and closes the ION writer and the output file stream, if still open.
     * @throws IOException If the output cannot be written or closed.
     */
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

    /**
     * Get the storage URI of a stored input.
     * @param side Side of input.
     * @return URI of input file.
     */
    private URI storageUri(JoinSide side) {
        return (side == JoinSide.LEFT ? leftInput : rightInput).storageUri();
    }

    /**
     * Fetch input file from Kestra storage and creates a ION reader from it.
     * @param inputUri URI of input file.
     * @return ION reader for the input, to be closed by the caller.
     * @throws TransformException If the file cannot be opened.
     */
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

    /**
     * Counts the rows of an input: list size for in-memory records, simple parsing for a stored file.
     * @param side Side of the input (left or right).
     * @return Number of rows.
     * @throws TransformException If the stored input cannot be read.
     */
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
