package io.kestra.plugin.transform.util;

public final class JoinMessages {

    private JoinMessages() {}

    public static final String RENDER_ERROR = "An exception occurred while trying to render '%s' property.";
    public static final String BLANK_PROPERTY = "The '%s' property rendered to a blank value.";
    public static final String SAME_LABELS = "The 'leftLabel' and 'rightLabel' properties must render to different values.";
    public static final String READ_ERROR = "An exception occurred while trying to read '%s' input.";
    public static final String EMPTY_INPUT = "The %s input is empty.";
    public static final String MISSING_KEY = "The key field '%s' is missing in the %s input at row %d.";
    public static final String DIFFERENT_TYPES = "Impossible to compare key values of different types: %s and %s.";
    public static final String UNPROCESSABLE_COMPARED_TYPES = "Impossible to compare the values because their type cannot be processed.";
    public static final String UNPROCESSABLE_KEY_TYPE = "The type of value for key '%s' from %s input cannot be processed.";
    public static final String UNREACHABLE_KEY_DEPTH = "The key depth in '%s' for %s input is not reachable.";
    public static final String KEY_VALUE_IS_OBJECT = "The value for key '%s' from %s input is an object. Hint: the key may need to be nested or deeper.";
    public static final String WRITE_ERROR = "An exception occurred while trying to write %s in temporary output file.";
    public static final String STORAGE_ERROR = "An exception occurred while trying to store output file.";
}
