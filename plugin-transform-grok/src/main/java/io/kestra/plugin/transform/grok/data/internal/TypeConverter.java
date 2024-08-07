package io.kestra.plugin.transform.grok.data.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Service class for converting Object of one type to another.
 *
 * @param <T> the target type.
 */
public final class TypeConverter<T> {

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = JsonMapper.builder()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true)
        .build();

    private final ObjectMapper objectMapper;
    private final JavaType type;

    /**
     * Creates a new converter for converting object into the given type.
     *
     * @param objectType Type of object.
     * @param <T>        Type of object.
     * @return The converter.
     */
    public static <T> TypeConverter<T> newForType(final TypeReference<T> objectType) {
        TypeFactory typeFactory = DEFAULT_OBJECT_MAPPER.getTypeFactory();
        JavaType type = typeFactory.constructType(objectType);
        return new TypeConverter<>(DEFAULT_OBJECT_MAPPER, type);
    }

    /**
     * Creates a new converter for converting object into the given type.
     *
     * @param objectType Type of object.
     * @param <T>        Type of object.
     * @return The converter.
     */
    public static <T> TypeConverter<T> newForType(final Class<T> objectType) {
        TypeFactory typeFactory = DEFAULT_OBJECT_MAPPER.getTypeFactory();
        JavaType type = typeFactory.constructType(objectType);
        return new TypeConverter<>(DEFAULT_OBJECT_MAPPER, type);
    }

    /**
     * Creates a new converter for converting object into a list of elements of the given type.
     *
     * @param elementClass Type of elements.
     * @param <T>          Type of object.
     * @return The converter.
     */
    public static <T> TypeConverter<List<T>> newForList(Class<T> elementClass) {
        TypeFactory typeFactory = DEFAULT_OBJECT_MAPPER.getTypeFactory();
        CollectionType type = typeFactory.constructCollectionType(List.class, elementClass);
        return new TypeConverter<>(DEFAULT_OBJECT_MAPPER, type);
    }

    /**
     * Creates a new converter for converting object into a list of elements of the given type.
     *
     * @param elementClass Type of elements.
     * @param <T>          Type of object.
     * @return The converter.
     */
    public static <T> TypeConverter<Set<T>> newForSet(Class<T> elementClass) {
        TypeFactory typeFactory = DEFAULT_OBJECT_MAPPER.getTypeFactory();
        CollectionType type = typeFactory.constructCollectionType(Set.class, elementClass);
        return new TypeConverter<>(DEFAULT_OBJECT_MAPPER, type);
    }

    /**
     * Creates a new {@link TypeConverter} instance.
     *
     * @param objectMapper The {@link ObjectMapper}.
     */
    public TypeConverter(ObjectMapper objectMapper, JavaType type) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
    }

    public T convertValue(Object value) {
        return value == null ? null : objectMapper.convertValue(value, type);
    }
}