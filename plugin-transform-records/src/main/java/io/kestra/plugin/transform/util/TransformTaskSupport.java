package io.kestra.plugin.transform.util;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.ion.IonValueUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public final class TransformTaskSupport {
    private TransformTaskSupport() {
    }

    public static ResolvedInput resolveInput(RunContext runContext, Property<Object> from) throws Exception {
        if (from == null) {
            return new ResolvedInput(null, false, null);
        }

        String expression = from.toString();
        if (expression != null && expression.contains("{{") && expression.contains("}}")) {
            Object typed = runContext.renderTyped(expression);
            if (typed != null && !(typed instanceof String)) {
                ResolvedInput resolved = resolveStorageCandidate(typed);
                if (resolved != null) {
                    return resolved;
                }
                return new ResolvedInput(typed, false, null);
            }
        }

        RunContextProperty<Object> rendered = runContext.render(from);
        Object value = rendered.as(Object.class).orElse(null);
        ResolvedInput resolved = resolveStorageCandidate(value);
        if (resolved != null) {
            return resolved;
        }
        if (!(value instanceof String)) {
            return new ResolvedInput(value, false, null);
        }
        try {
            Object listValue = rendered.asList(Object.class);
            if (listValue instanceof List) {
                return new ResolvedInput(listValue, false, null);
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        try {
            Object mapValue = rendered.asMap(String.class, Object.class);
            if (mapValue instanceof Map) {
                return new ResolvedInput(mapValue, false, null);
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        return new ResolvedInput(value, false, null);
    }

    private static ResolvedInput resolveStorageCandidate(Object value) {
        if (value instanceof URI uriValue) {
            if (uriValue.getScheme() == null) {
                return new ResolvedInput(value, false, null);
            }
            return new ResolvedInput(uriValue, true, uriValue);
        }
        if (value instanceof String stringValue) {
            URI uri;
            try {
                uri = URI.create(stringValue);
            } catch (IllegalArgumentException e) {
                return null;
            }
            if (uri.getScheme() == null) {
                return null;
            }
            return new ResolvedInput(uri, true, uri);
        }
        return null;
    }

    public static List<IonStruct> normalizeRecords(Object rendered) throws TransformException {
        if (rendered == null) {
            return List.of();
        }
        if (rendered instanceof IonStruct ionStruct) {
            return List.of(ionStruct);
        }
        if (rendered instanceof IonList ionList) {
            List<IonStruct> records = new ArrayList<>();
            for (IonValue value : ionList) {
                records.add(asStruct(value));
            }
            return records;
        }
        if (rendered instanceof List<?> list) {
            List<IonStruct> records = new ArrayList<>();
            for (Object value : list) {
                records.add(asStruct(IonValueUtils.toIonValue(value)));
            }
            return records;
        }
        if (rendered instanceof Map<?, ?> map) {
            IonStruct struct = asStruct(IonValueUtils.toIonValue(map));
            return List.of(struct);
        }
        throw new TransformException("Unsupported input type: " + rendered.getClass().getName()
            + ". The 'from' property must resolve to a list/map of records, Ion values, or a storage URI.");
    }

    public static Object loadIonFromStorage(RunContext runContext, URI uri) throws TransformException {
        try (InputStream inputStream = runContext.storage().getFile(uri)) {
            IonList list = IonValueUtils.system().newEmptyList();
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                list.add(IonValueUtils.cloneValue(iterator.next()));
            }
            return unwrapIonList(list);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }
    }

    public static OutputStream bufferedOutput(java.nio.file.Path outputPath) throws IOException {
        return new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(outputPath), FileSerde.BUFFER_SIZE);
    }

    public static OutputStream wrapCompression(OutputStream outputStream) throws IOException {
        String compression = System.getProperty("bench.compression");
        if (compression != null && "gzip".equalsIgnoreCase(compression.trim())) {
            return new GZIPOutputStream(outputStream, true);
        }
        return outputStream;
    }

    public static IonWriter createWriter(OutputStream outputStream, OutputFormat format) throws IOException {
        if (format == OutputFormat.BINARY) {
            return IonBinaryWriterBuilder.standard().build(outputStream);
        }
        return IonValueUtils.system().newTextWriter(outputStream);
    }

    public static void writeDelimiter(OutputStream outputStream, OutputFormat format) throws IOException {
        if (format == OutputFormat.TEXT) {
            outputStream.write('\n');
        }
    }

    private static IonStruct asStruct(IonValue value) throws TransformException {
        if (value instanceof IonStruct struct) {
            return struct;
        }
        throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
    }

    private static Object unwrapIonList(IonList list) {
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        if (list.size() == 1) {
            IonValue value = list.get(0);
            if (value instanceof IonStruct || value instanceof IonList) {
                return value;
            }
        }
        return list;
    }

    public record ResolvedInput(Object value, boolean fromStorage, URI storageUri) {
    }
}
