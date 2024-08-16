package io.kestra.plugin.transform.grok.pattern;

import io.kestra.plugin.transform.grok.data.Type;
import org.joni.Region;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * GrokCaptureGroup.
 *
 * @param type     the type defined for the data field to capture.
 * @param name     the name defined for the data field to capture.
 * @param backRefs
 */
public record GrokCaptureGroup(
    Type type,
    String name,
    int[] backRefs
) {

    public GrokCaptureGroup {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
    }


    /**
     * Gets the {@link GrokCaptureExtractor} to be used for capturing that group.
     *
     * @param consumer the {@link Consumer} to call when a data field is captured.
     * @return the GrokCaptureExtractor
     */
    public GrokCaptureExtractor getExtractor(final Consumer<Object> consumer) {
        return new RawValueExtractor(backRefs, (s -> consumer.accept(type.convert(s))));
    }

    private record RawValueExtractor(int[] backRefs, Consumer<String> consumer) implements GrokCaptureExtractor {

        private static final String EMPTY_VALUE = "";

        /**
         * {@inheritDoc}
         */
        @Override
        public void extract(byte[] bytes, Region region) {
            for (int capture : backRefs) {
                int offset = region.getBeg(capture);
                int length = region.getEnd(capture) - offset;
                if (offset >= 0) {
                    String value = new String(bytes, offset, length, StandardCharsets.UTF_8);
                    consumer.accept(value);
                    break; // we only need to capture the first value.
                } else {
                    consumer.accept(EMPTY_VALUE);
                }
            }
        }
    }
}
