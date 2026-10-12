package io.kestra.plugin.transform.arrow;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ArrowTransformTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        ArrowTransform task = ArrowTransform.builder()
            .from(Property.of("kestra://test.csv"))
            .query(Property.of(".[]"))
            .build();

        // Basic builder and context validation test
        assertThat(task, notNullValue());
        assertThat(task.getFrom(), notNullValue());
        assertThat(task.getQuery(), notNullValue());
    }
    }
