package io.kestra.plugin.transform;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;

import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.ion.IterableIonReader;
import io.kestra.plugin.transform.util.JoinImplementation;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.MergeMode;
import io.kestra.plugin.transform.util.MissingKeyAction;
import io.kestra.plugin.transform.util.JoinMessages;
import io.kestra.plugin.transform.util.OutputFormat;

import com.amazon.ion.IonValue;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.validations.ModelValidator;

import jakarta.inject.Inject;

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class JoinTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private ModelValidator modelValidator;

    private String defaultKey = "id";

    private String defaultLeftContent = """
        {id:1,leftValue:"L1"}
        {id:2,leftValue:"L2-A"}
        {id:2,leftValue:"L2-B"}
        {id:3,leftValue:"L3"}
        {id:5,leftValue:"L5"}""";

    private String defaultRightContent = """
        {id:2,rightValue:"R2-A"}
        {id:2,rightValue:"R2-B"}
        {id:3,rightValue:"R3"}
        {id:4,rightValue:"R4"}
        {id:5,rightValue:"R5-A"}
        {id:5,rightValue:"R5-B"}""";

    private Map<JoinType, String> defaultExpectedResults =
        Map.of(
            JoinType.FULL_OUTER, """
                {id:1,leftValue:"L1"}
                {id:2,leftValue:"L2-A",rightValue:"R2-A"}
                {id:2,leftValue:"L2-A",rightValue:"R2-B"}
                {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                {id:2,leftValue:"L2-B",rightValue:"R2-B"}
                {id:3,leftValue:"L3",rightValue:"R3"}
                {id:4,rightValue:"R4"}
                {id:5,leftValue:"L5",rightValue:"R5-A"}
                {id:5,leftValue:"L5",rightValue:"R5-B"}"""
            ,
            JoinType.INNER, """
                {id:2,leftValue:"L2-A",rightValue:"R2-A"}
                {id:2,leftValue:"L2-A",rightValue:"R2-B"}
                {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                {id:2,leftValue:"L2-B",rightValue:"R2-B"}
                {id:3,leftValue:"L3",rightValue:"R3"}
                {id:5,leftValue:"L5",rightValue:"R5-A"}
                {id:5,leftValue:"L5",rightValue:"R5-B"}"""
            ,
            JoinType.LEFT_ANTI, """
                {id:1,leftValue:"L1"}"""
            ,
            JoinType.LEFT_OUTER, """
                {id:1,leftValue:"L1"}
                {id:2,leftValue:"L2-A",rightValue:"R2-A"}
                {id:2,leftValue:"L2-A",rightValue:"R2-B"}
                {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                {id:2,leftValue:"L2-B",rightValue:"R2-B"}
                {id:3,leftValue:"L3",rightValue:"R3"}
                {id:5,leftValue:"L5",rightValue:"R5-A"}
                {id:5,leftValue:"L5",rightValue:"R5-B"}"""
            ,
            JoinType.LEFT_SEMI, """
                {id:2,leftValue:"L2-A"}
                {id:2,leftValue:"L2-A"}
                {id:2,leftValue:"L2-B"}
                {id:2,leftValue:"L2-B"}
                {id:3,leftValue:"L3"}
                {id:5,leftValue:"L5"}
                {id:5,leftValue:"L5"}"""
            ,
            JoinType.RIGHT_ANTI, """
                {id:4,rightValue:"R4"}"""
            ,
            JoinType.RIGHT_OUTER, """
                {id:2,leftValue:"L2-A",rightValue:"R2-A"}
                {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                {id:2,leftValue:"L2-A",rightValue:"R2-B"}
                {id:2,leftValue:"L2-B",rightValue:"R2-B"}
                {id:3,leftValue:"L3",rightValue:"R3"}
                {id:4,rightValue:"R4"}
                {id:5,leftValue:"L5",rightValue:"R5-A"}
                {id:5,leftValue:"L5",rightValue:"R5-B"}"""
            ,
            JoinType.RIGHT_SEMI, """
                {id:2,rightValue:"R2-A"}
                {id:2,rightValue:"R2-A"}
                {id:2,rightValue:"R2-B"}
                {id:2,rightValue:"R2-B"}
                {id:3,rightValue:"R3"}
                {id:5,rightValue:"R5-A"}
                {id:5,rightValue:"R5-B"}"""
    );

    private String tenant = TenantService.MAIN_TENANT;

    /**
     * Prepare Kestra run context by creating ION inputs files from strings.
     * @param leftContent string of left input.
     * @param rightContent string of right input.
     * @return Run context storing files, ready to run task.
     */
    private RunContext getRunContext(String leftContent, String rightContent) {
        return this.runContextFactory.of(Map.of(
            "left", storeContent(leftContent).toString(),
            "right", storeContent(rightContent).toString()
        ));
    }

    /**
     * Store an ION string as a file in Kestra storage.
     * @param content string of input.
     * @return URI of the stored file.
     */
    private URI storeContent(String content) {
        try {
            return this.storageInterface.put(
                tenant,
                null,
                URI.create("/" + IdUtils.create() + ".ion"),
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
            );
        } catch (IOException e) {
            return fail("Unable to store test input file.", e);
        }
    }

    /**
     * Read the output file of a task run.
     * @param output Output of the task run.
     * @return Content of the output file.
     * @throws IOException If the file cannot be read from storage.
     */
    private String readOutput(Join.Output output) throws IOException {
        try (InputStream stream = this.storageInterface.get(tenant, null, output.getUri())) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Builder of a valid task with given keys, reading the "left" and "right" variables of the run context.
     * Each task gets a random id: Kestra requires an id on every task, and a unique one keeps
     * runs, logs and outputs of different tests apart.
     * @see getRunContext()
     * @param leftKey
     * @param rightKey
     * @return Task builder with id, type, inputs and keys.
     */
    private Join.JoinBuilder<?, ?> taskBuilder(String leftKey, String rightKey) {
        return Join.builder()
            .id(IdUtils.create())
            .type(Join.class.getName())
            .left(Property.ofExpression("{{ left }}"))
            .right(Property.ofExpression("{{ right }}"))
            .leftKey(Property.ofValue(leftKey))
            .rightKey(Property.ofValue(rightKey));
    }

    /**
     * Run task with mandatory properties only and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param leftKey
     * @param rightKey
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            String leftKey,
            String rightKey) throws Exception {
        Join task = taskBuilder(leftKey, rightKey)
            .joinImplementation(Property.ofValue(joinImplementation))
            .joinType(Property.ofValue(joinType))
            .build();
        return task.run(runContext);
    }

    /**
     * Run task with labels and output mode properties and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param leftKey
     * @param rightKey
     * @param leftLabel
     * @param rightLabel
     * @param mergeMode
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            String leftKey,
            String rightKey,
            String leftLabel,
            String rightLabel,
            MergeMode mergeMode) throws Exception {
        Join task = taskBuilder(leftKey, rightKey)
            .joinImplementation(Property.ofValue(joinImplementation))
            .joinType(Property.ofValue(joinType))
            .leftLabel(leftLabel)
            .rightLabel(rightLabel)
            .mergeMode(Property.ofValue(mergeMode))
            .build();
        return task.run(runContext);
    }

    /**
     * Run task with onlyFirstMatchOnSemi optional property and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param leftKey
     * @param rightKey
     * @param onlyFirstMatchOnSemi
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            String leftKey,
            String rightKey,
            Boolean onlyFirstMatchOnSemi) throws Exception {
        Join task = taskBuilder(leftKey, rightKey)
            .joinImplementation(Property.ofValue(joinImplementation))
            .joinType(Property.ofValue(joinType))
            .onlyFirstMatchOnSemi(Property.ofValue(onlyFirstMatchOnSemi))
            .build();
        return task.run(runContext);
    }

    /**
     * Run task with onMissingKey optional property and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param leftKey
     * @param rightKey
     * @param onMissingKey
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            String leftKey,
            String rightKey,
            MissingKeyAction onMissingKey) throws Exception {
        Join task = taskBuilder(leftKey, rightKey)
            .joinImplementation(Property.ofValue(joinImplementation))
            .joinType(Property.ofValue(joinType))
            .onMissingKey(Property.ofValue(onMissingKey))
            .build();
        return task.run(runContext);
    }

    /**
     * Run task with default keys and no optional properties and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType) throws Exception {
        return getOutput(runContext,
            joinImplementation,
            joinType,
            defaultKey,
            defaultKey);
    }

    /**
     * Run task with default keys, labels and output mode properties and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param leftLabel
     * @param rightLabel
     * @param mergeMode
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutputWithLabels(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            String leftLabel,
            String rightLabel,
            MergeMode mergeMode) throws Exception {
        return getOutput(runContext,
            joinImplementation,
            joinType,
            defaultKey,
            defaultKey,
            leftLabel,
            rightLabel,
            mergeMode);
    }

    /**
     * Run task with default keys and onlyFirstMatchOnSemi optional property and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param onlyFirstMatchOnSemi
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutputWithOnlyFirstMatchOnSemiOnly(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            Boolean onlyFirstMatchOnSemi) throws Exception {
        return getOutput(runContext,
            joinImplementation,
            joinType,
            defaultKey,
            defaultKey,
            onlyFirstMatchOnSemi);
    }

    /**
     * Run task with default keys and onMissingKey optional property and return output.
     * Left and right data are not passed as parameters but as files with run context.
     * @see getRunContext()
     * @param joinImplementation
     * @param joinType
     * @param onMissingKey
     * @return Output object.
     * @throws Exception
     */
    private Join.Output getOutput(RunContext runContext,
            JoinImplementation joinImplementation,
            JoinType joinType,
            MissingKeyAction onMissingKey) throws Exception {
        return getOutput(runContext, joinImplementation, joinType, defaultKey, defaultKey, onMissingKey);
    }

    /**
     * Sort lines of ION string.
     * @param content Unsorted content.
     * @return Sorted content.
     */
    private String sortIonLines(String content) {
        return Arrays.stream(content.strip().split("\\R"))
            .filter(line -> !line.isBlank())
            .sorted()
            .collect(Collectors.joining("\n"));
    }

    /**
     * Describe a test case in assertion messages.
     * @param joinImplementation
     * @param joinType
     * @return Description of the case.
     */
    private String describe(JoinImplementation joinImplementation, JoinType joinType) {
        return " (with " + joinImplementation + ", " + joinType + " join)";
    }

    /*
    ---------------------------------------------------------------------------------------
    Tests of minimum required behaviour with basic usage
    ---------------------------------------------------------------------------------------
    */

    /**
     * Test of join algorithms consistency by checking result data for each join type with basic dataset and no option.
     * Given and expected results are sorted before comparison to eliminate differences of line order.
     * Implementations only affects line order so the expected results doesn't differ according to this setting.
     */
    @Test
    void joinResultsAreConsistent() throws Exception {

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(defaultExpectedResults.get(joinType));

                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should be consistent according to join type" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Tests that counts outputs are available and correct according to dataset.
     */
    @Test
    void countsAreCorrect() throws Exception {

        long expectedLeftSize = defaultLeftContent.split("\n").length;
        long expectedRightSize = defaultRightContent.split("\n").length;

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                long expectedResultCount = defaultExpectedResults.get(joinType).split("\n").length;

                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType);

                assertThat("Output count should be equal to expected result count" + describe(joinImplementation, joinType),
                    runOutput.getOutputSize(), equalTo(expectedResultCount));
                assertThat("Left count should be correct" + describe(joinImplementation, joinType),
                    runOutput.getLeftSize(), equalTo(expectedLeftSize));
                assertThat("Right count should be correct" + describe(joinImplementation, joinType),
                    runOutput.getRightSize(), equalTo(expectedRightSize));
            }
        }
    }

    /**
     * Tests that task don't fail if one or both data input(s) is empty.
     * Empty inputs are not strictly invalid for join algorithms.
     * This test checks that this use case is not broken, for example by other piece of code than join algorithms.
     */
    @Test
    void emptyContentIsValid() {

        Map<String, String[]> contentsBySide =
            Map.of(
                "LEFT", new String[] { "", defaultRightContent },
                "RIGHT", new String[] { defaultLeftContent, "" },
                "BOTH", new String[] { "", "" }
            );

        for (var testEntry : contentsBySide.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                for (JoinType joinType : JoinType.values()) {
                    assertDoesNotThrow(
                        () -> getOutput(runContext, joinImplementation, joinType),
                        "Should not fail with " + testEntry.getKey() + " empty input(s)" + describe(joinImplementation, joinType)
                    );
                }
            }
        }
    }

    /**
     * Tests that a leftKey or rightKey property rendering to an empty or blank string fails
     * with a render error naming the property, instead of a missing key error or an empty result.
     */
    @Test
    void blankKeyThrowRenderError() {

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                for (String blankValue : new String[] { "", "   " }) {
                    Map<String, String[]> keysByProperty =
                        Map.of(
                            "leftKey", new String[] { blankValue, defaultKey },
                            "rightKey", new String[] { defaultKey, blankValue }
                        );

                    for (var testEntry : keysByProperty.entrySet()) {
                        String expectedError = String.format(JoinMessages.BLANK_PROPERTY, testEntry.getKey());

                        Exception exception = assertThrows(Exception.class, () -> getOutput(runContext,
                            joinImplementation,
                            joinType,
                            testEntry.getValue()[0],
                            testEntry.getValue()[1]));

                        assertThat("Should show error for blank value [" + blankValue + "] on " + testEntry.getKey() + describe(joinImplementation, joinType),
                            exception.getMessage(), equalTo(expectedError));
                    }
                }
            }
        }
    }

    /**
     * Tests that a left or right property rendering to an empty or blank string fails
     * with a render error naming the property, instead of a storage read error.
     */
    @Test
    void blankInputThrowRenderError() {

        String leftFile = storeContent(defaultLeftContent).toString();
        String rightFile = storeContent(defaultRightContent).toString();

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                for (String blankValue : new String[] { "", "   " }) {
                    Map<String, String[]> inputsByProperty =
                        Map.of(
                            "left", new String[] { blankValue, rightFile },
                            "right", new String[] { leftFile, blankValue }
                        );

                    for (var testEntry : inputsByProperty.entrySet()) {
                        String expectedError = String.format(JoinMessages.BLANK_PROPERTY, testEntry.getKey());

                        RunContext runContext = this.runContextFactory.of(Map.of(
                            "left", testEntry.getValue()[0],
                            "right", testEntry.getValue()[1]
                        ));

                        Exception exception = assertThrows(Exception.class,
                            () -> getOutput(runContext, joinImplementation, joinType));

                        assertThat("Should show error for blank value [" + blankValue + "] on " + testEntry.getKey() + describe(joinImplementation, joinType),
                            exception.getMessage(), equalTo(expectedError));
                    }
                }
            }
        }
    }

    /**
     * Tests that optional properties rendering to nothing fall back to their default values instead of failing.
     */
    @Test
    void emptyOptionalPropertiesFallBackToDefaults() throws Exception {

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        Join task = taskBuilder(defaultKey, defaultKey)
            .joinImplementation(null)
            .joinType(null)
            .mergeMode(null)
            .onlyFirstMatchOnSemi(null)
            .onMissingKey(null)
            .outputFormat(null)
            .outputType(null)
            .build();

        Join.Output runOutput = task.run(runContext);

        assertThat("Inputs from storage should give a stored output by default", runOutput.getUri(), notNullValue());
        assertThat("Result should be the one of the default join type and merge mode",
            sortIonLines(readOutput(runOutput)), equalTo(sortIonLines(defaultExpectedResults.get(JoinType.INNER))));
    }

    /**
     * Tests that accentuated characters are preserved.
     */
    @Test
    void handleAccents() throws Exception {

        String leftContent = """
            {id:1,leftValue:"é"}
            {id:2,leftValue:"à"}
            {id:2,leftValue:"ê"}
            {id:3,leftValue:"ç"}
            {id:5,leftValue:"ù"}""";

        String rightContent = """
            {id:2,rightValue:"ã"}
            {id:2,rightValue:"â"}
            {id:3,rightValue:"è"}
            {id:4,rightValue:"ô"}
            {id:5,rightValue:"ï"}
            {id:5,rightValue:"ü"}""";

        Map<JoinType, String> expectedResults =
            Map.of(
                JoinType.FULL_OUTER, """
                    {id:2,leftValue:"à",rightValue:"ã"}
                    {id:2,leftValue:"ê",rightValue:"ã"}
                    {id:2,leftValue:"à",rightValue:"â"}
                    {id:2,leftValue:"ê",rightValue:"â"}
                    {id:3,leftValue:"ç",rightValue:"è"}
                    {id:4,rightValue:"ô"}
                    {id:5,leftValue:"ù",rightValue:"ï"}
                    {id:5,leftValue:"ù",rightValue:"ü"}
                    {id:1,leftValue:"é"}"""
                ,
                JoinType.INNER, """
                    {id:2,leftValue:"à",rightValue:"ã"}
                    {id:2,leftValue:"ê",rightValue:"ã"}
                    {id:2,leftValue:"à",rightValue:"â"}
                    {id:2,leftValue:"ê",rightValue:"â"}
                    {id:3,leftValue:"ç",rightValue:"è"}
                    {id:5,leftValue:"ù",rightValue:"ï"}
                    {id:5,leftValue:"ù",rightValue:"ü"}"""
                ,
                JoinType.LEFT_ANTI, """
                    {id:1,leftValue:"é"}"""
                ,
                JoinType.LEFT_OUTER, """
                    {id:2,leftValue:"à",rightValue:"ã"}
                    {id:2,leftValue:"ê",rightValue:"ã"}
                    {id:2,leftValue:"à",rightValue:"â"}
                    {id:2,leftValue:"ê",rightValue:"â"}
                    {id:3,leftValue:"ç",rightValue:"è"}
                    {id:5,leftValue:"ù",rightValue:"ï"}
                    {id:5,leftValue:"ù",rightValue:"ü"}
                    {id:1,leftValue:"é"}"""
                ,
                JoinType.LEFT_SEMI, """
                    {id:2,leftValue:"à"}
                    {id:2,leftValue:"ê"}
                    {id:2,leftValue:"à"}
                    {id:2,leftValue:"ê"}
                    {id:3,leftValue:"ç"}
                    {id:5,leftValue:"ù"}
                    {id:5,leftValue:"ù"}"""
                ,
                JoinType.RIGHT_ANTI, """
                    {id:4,rightValue:"ô"}"""
                ,
                JoinType.RIGHT_OUTER, """
                    {id:2,leftValue:"à",rightValue:"ã"}
                    {id:2,leftValue:"ê",rightValue:"ã"}
                    {id:2,leftValue:"à",rightValue:"â"}
                    {id:2,leftValue:"ê",rightValue:"â"}
                    {id:3,leftValue:"ç",rightValue:"è"}
                    {id:4,rightValue:"ô"}
                    {id:5,leftValue:"ù",rightValue:"ï"}
                    {id:5,leftValue:"ù",rightValue:"ü"}"""
                ,
                JoinType.RIGHT_SEMI, """
                    {id:2,rightValue:"ã"}
                    {id:2,rightValue:"â"}
                    {id:2,rightValue:"ã"}
                    {id:2,rightValue:"â"}
                    {id:3,rightValue:"è"}
                    {id:5,rightValue:"ï"}
                    {id:5,rightValue:"ü"}"""
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(expectedResults.get(joinType));

                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should preserve accents" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Tests that string values containing "} {" are written unchanged.
     * Regression test: rows used to be split on this sequence in the whole output text, values included.
     */
    @Test
    void curlyBracesInValuesArePreserved() throws Exception {

        String leftContent = """
            {id:1,leftValue:"a} {b"}
            {id:2,leftValue:"} {"}""";

        String rightContent = """
            {id:1,rightValue:"x"}
            {id:2,rightValue:"y"}""";

        String expected = sortIonLines("""
            {id:1,leftValue:"a} {b",rightValue:"x"}
            {id:2,leftValue:"} {",rightValue:"y"}""");

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Join.Output runOutput = getOutput(runContext, joinImplementation, JoinType.INNER);
            String result = sortIonLines(readOutput(runOutput));

            assertThat("Values containing \"} {\" should be preserved" + describe(joinImplementation, JoinType.INNER),
                result, equalTo(expected));
        }
    }

    /*
    ---------------------------------------------------------------------------------------
    Tests of extended features
    ---------------------------------------------------------------------------------------
    */

    /**
     * Test of join with nested keys by using the . sign in keys properties.
     */
    @Test
    void nestedKeysAreManaged() throws Exception {

        String leftContent = """
            {object:{id:1},leftValue:"L1"}
            {object:{id:2},leftValue:"L2-A"}
            {object:{id:2},leftValue:"L2-B"}
            {object:{id:3},leftValue:"L3"}
            {object:{id:5},leftValue:"L5"}""";

        String rightContent = """
            {object:{id:2},rightValue:"R2-A"}
            {object:{id:2},rightValue:"R2-B"}
            {object:{id:3},rightValue:"R3"}
            {object:{id:4},rightValue:"R4"}
            {object:{id:5},rightValue:"R5-A"}
            {object:{id:5},rightValue:"R5-B"}""";

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(defaultExpectedResults.get(joinType));

                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType, "object.id", "object.id");

                // Nested field is replaced by a plain one, so the result can be compared to default expected results
                String result = sortIonLines(readOutput(runOutput)
                    .replaceAll("object:\\{id:([^}]+)}", "id:$1"));

                assertThat("Result should be joined by using nested keys" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Test of fields renaming in result by using prefix merge mode, including deduplication of common fields.
     */
    @Test
    void withPrefixesFieldsAreRenamed() throws Exception {

        String leftContent = """
            {id:1,value:"L1"}
            {id:2,value:"L2-A"}
            {id:2,value:"L2-B"}
            {id:3,value:"L3"}
            {id:5,value:"L5"}""";

        String rightContent = """
            {id:2,value:"R2-A"}
            {id:2,value:"R2-B"}
            {id:3,value:"R3"}
            {id:4,value:"R4"}
            {id:5,value:"R5-A"}
            {id:5,value:"R5-B"}""";

        Map<JoinType, String> prefixedExpectedResults =
            Map.of(
                JoinType.FULL_OUTER, """
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-B"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-B"}
                    {L_id:3,L_value:"L3",R_id:3,R_value:"R3"}
                    {R_id:4,R_value:"R4"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-A"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-B"}
                    {L_id:1,L_value:"L1"}"""
                ,
                JoinType.INNER, """
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-B"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-B"}
                    {L_id:3,L_value:"L3",R_id:3,R_value:"R3"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-A"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-B"}"""
                ,
                JoinType.LEFT_ANTI, """
                    {L_id:1,L_value:"L1"}"""
                ,
                JoinType.LEFT_OUTER, """
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-B"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-B"}
                    {L_id:3,L_value:"L3",R_id:3,R_value:"R3"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-A"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-B"}
                    {L_id:1,L_value:"L1"}"""
                ,
                JoinType.LEFT_SEMI, """
                    {L_id:2,L_value:"L2-A"}
                    {L_id:2,L_value:"L2-B"}
                    {L_id:2,L_value:"L2-A"}
                    {L_id:2,L_value:"L2-B"}
                    {L_id:3,L_value:"L3"}
                    {L_id:5,L_value:"L5"}
                    {L_id:5,L_value:"L5"}"""
                ,
                JoinType.RIGHT_ANTI, """
                    {R_id:4,R_value:"R4"}"""
                ,
                JoinType.RIGHT_OUTER, """
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-A"}
                    {L_id:2,L_value:"L2-A",R_id:2,R_value:"R2-B"}
                    {L_id:2,L_value:"L2-B",R_id:2,R_value:"R2-B"}
                    {L_id:3,L_value:"L3",R_id:3,R_value:"R3"}
                    {R_id:4,R_value:"R4"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-A"}
                    {L_id:5,L_value:"L5",R_id:5,R_value:"R5-B"}"""
                ,
                JoinType.RIGHT_SEMI, """
                    {R_id:2,R_value:"R2-A"}
                    {R_id:2,R_value:"R2-A"}
                    {R_id:2,R_value:"R2-B"}
                    {R_id:2,R_value:"R2-B"}
                    {R_id:3,R_value:"R3"}
                    {R_id:5,R_value:"R5-A"}
                    {R_id:5,R_value:"R5-B"}"""
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(prefixedExpectedResults.get(joinType));

                Join.Output runOutput = getOutputWithLabels(runContext, joinImplementation, joinType, "L", "R", MergeMode.PREFIX);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should have renamed fields from left and right" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Test of nest merge mode.
     */
    @Test
    void nestMergeMode() throws Exception {

        String leftContent = """
            {id:1,value:"L1"}
            {id:2,value:"L2-A"}
            {id:2,value:"L2-B"}
            {id:3,value:"L3"}
            {id:5,value:"L5"}""";

        String rightContent = """
            {id:2,value:"R2-A"}
            {id:2,value:"R2-B"}
            {id:3,value:"R3"}
            {id:4,value:"R4"}
            {id:5,value:"R5-A"}
            {id:5,value:"R5-B"}""";

        Map<JoinType, String> nestedExpectedResults =
            Map.of(
                JoinType.FULL_OUTER, """
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-B"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-B"}}
                    {left:{id:3,value:"L3"},right:{id:3,value:"R3"}}
                    {right:{id:4,value:"R4"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-A"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-B"}}
                    {left:{id:1,value:"L1"}}"""
                ,
                JoinType.INNER, """
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-B"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-B"}}
                    {left:{id:3,value:"L3"},right:{id:3,value:"R3"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-A"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-B"}}"""
                ,
                JoinType.LEFT_ANTI, """
                    {left:{id:1,value:"L1"}}"""
                ,
                JoinType.LEFT_OUTER, """
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-B"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-B"}}
                    {left:{id:3,value:"L3"},right:{id:3,value:"R3"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-A"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-B"}}
                    {left:{id:1,value:"L1"}}"""
                ,
                JoinType.LEFT_SEMI, """
                    {left:{id:2,value:"L2-A"}}
                    {left:{id:2,value:"L2-B"}}
                    {left:{id:2,value:"L2-A"}}
                    {left:{id:2,value:"L2-B"}}
                    {left:{id:3,value:"L3"}}
                    {left:{id:5,value:"L5"}}
                    {left:{id:5,value:"L5"}}"""
                ,
                JoinType.RIGHT_ANTI, """
                    {right:{id:4,value:"R4"}}"""
                ,
                JoinType.RIGHT_OUTER, """
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-A"}}
                    {left:{id:2,value:"L2-A"},right:{id:2,value:"R2-B"}}
                    {left:{id:2,value:"L2-B"},right:{id:2,value:"R2-B"}}
                    {left:{id:3,value:"L3"},right:{id:3,value:"R3"}}
                    {right:{id:4,value:"R4"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-A"}}
                    {left:{id:5,value:"L5"},right:{id:5,value:"R5-B"}}"""
                ,
                JoinType.RIGHT_SEMI, """
                    {right:{id:2,value:"R2-A"}}
                    {right:{id:2,value:"R2-A"}}
                    {right:{id:2,value:"R2-B"}}
                    {right:{id:2,value:"R2-B"}}
                    {right:{id:3,value:"R3"}}
                    {right:{id:5,value:"R5-A"}}
                    {right:{id:5,value:"R5-B"}}"""
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(nestedExpectedResults.get(joinType));

                Join.Output runOutput = getOutputWithLabels(runContext, joinImplementation, joinType, "left", "right", MergeMode.NEST);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should be nested" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Test of the 'onlyFirstMatchOnSemi' option property as true.
     * This should prevent duplicate lines in result with semi joins.
     */
    @Test
    void onlyFirstMatchOnSemiOptionIsManaged() throws Exception {

        Map<JoinType, String> onlyFirstMatchExpectedResults =
            Map.of(
                JoinType.LEFT_SEMI, """
                    {id:2,leftValue:"L2-A"}
                    {id:2,leftValue:"L2-B"}
                    {id:3,leftValue:"L3"}
                    {id:5,leftValue:"L5"}"""
                ,
                JoinType.RIGHT_SEMI, """
                    {id:2,rightValue:"R2-A"}
                    {id:2,rightValue:"R2-B"}
                    {id:3,rightValue:"R3"}
                    {id:5,rightValue:"R5-A"}
                    {id:5,rightValue:"R5-B"}"""
            );

        JoinType[] semiJoinTypes = {
            JoinType.LEFT_SEMI,
            JoinType.RIGHT_SEMI
        };

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : semiJoinTypes) {
                String expected = sortIonLines(onlyFirstMatchExpectedResults.get(joinType));

                Join.Output runOutput = getOutputWithOnlyFirstMatchOnSemiOnly(runContext, joinImplementation, joinType, true);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should have only first matches with semi joins by using corresponding option" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Tests that task don't fail if onMissingKey option is IGNORE, and null values never match.
     */
    @Test
    void ignoredMissingKey() throws Exception {

        String leftContent = """
            {id:1,leftValue:"L1"}
            {id:null,leftValue:"L2-A"}
            {id:2,leftValue:"L2-B"}
            {leftValue:"L3"}
            {id:5,leftValue:"L5"}""";

        String rightContent = """
            {id:2,rightValue:"R2-A"}
            {id:null,rightValue:"R2-B"}
            {id:3,rightValue:"R3"}
            {id:4,rightValue:"R4"}
            {id:5,rightValue:"R5-A"}
            {id:5,rightValue:"R5-B"}""";

        Map<JoinType, String> expectedResults =
            Map.of(
                JoinType.FULL_OUTER, """
                    {id:1,leftValue:"L1"}
                    {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                    {id:3,rightValue:"R3"}
                    {id:4,rightValue:"R4"}
                    {id:5,leftValue:"L5",rightValue:"R5-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-B"}
                    {id:null,leftValue:"L2-A"}
                    {leftValue:"L3"}
                    {id:null,rightValue:"R2-B"}"""
                ,
                JoinType.INNER, """
                    {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-B"}"""
                ,
                JoinType.LEFT_ANTI, """
                    {leftValue:"L3"}
                    {id:null,leftValue:"L2-A"}
                    {id:1,leftValue:"L1"}"""
                ,
                JoinType.LEFT_OUTER, """
                    {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-B"}
                    {leftValue:"L3"}
                    {id:null,leftValue:"L2-A"}
                    {id:1,leftValue:"L1"}"""
                ,
                JoinType.LEFT_SEMI, """
                    {id:2,leftValue:"L2-B"}
                    {id:5,leftValue:"L5"}
                    {id:5,leftValue:"L5"}"""
                ,
                JoinType.RIGHT_ANTI, """
                    {id:null,rightValue:"R2-B"}
                    {id:3,rightValue:"R3"}
                    {id:4,rightValue:"R4"}"""
                ,
                JoinType.RIGHT_OUTER, """
                    {id:2,leftValue:"L2-B",rightValue:"R2-A"}
                    {id:3,rightValue:"R3"}
                    {id:4,rightValue:"R4"}
                    {id:5,leftValue:"L5",rightValue:"R5-A"}
                    {id:5,leftValue:"L5",rightValue:"R5-B"}
                    {id:null,rightValue:"R2-B"}"""
                ,
                JoinType.RIGHT_SEMI, """
                    {id:2,rightValue:"R2-A"}
                    {id:5,rightValue:"R5-A"}
                    {id:5,rightValue:"R5-B"}"""
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                String expected = sortIonLines(expectedResults.get(joinType));

                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType, MissingKeyAction.IGNORE);
                String result = sortIonLines(readOutput(runOutput));

                assertThat("Result should have no match between null values" + describe(joinImplementation, joinType),
                    result, equalTo(expected));
            }
        }
    }

    /**
     * Tests the task final state according to onMissingKey option when keys are missing:
     * WARNING must end the task in WARNING state, IGNORE must not change the state.
     * Both inputs have several rows with missing or null keys, so that every implementation extracts them.
     */
    @Test
    void missingKeyFinalStateDependsOnOption() throws Exception {

        String leftContent = """
            {id:1,leftValue:"L1"}
            {id:null,leftValue:"L2-A"}
            {id:2,leftValue:"L2-B"}
            {leftValue:"L3"}
            {id:5,leftValue:"L5"}""";

        String rightContent = """
            {id:2,rightValue:"R2-A"}
            {id:null,rightValue:"R2-B"}
            {id:3,rightValue:"R3"}
            {rightValue:"R4"}
            {id:5,rightValue:"R5-A"}""";

        Map<MissingKeyAction, Optional<State.Type>> expectedStates =
            Map.of(
                MissingKeyAction.WARNING, Optional.of(State.Type.WARNING),
                MissingKeyAction.IGNORE, Optional.empty()
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (var testEntry : expectedStates.entrySet()) {
            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                for (JoinType joinType : JoinType.values()) {
                    Join.Output runOutput = getOutput(runContext, joinImplementation, joinType, testEntry.getKey());

                    assertThat(
                        "Final state with " + testEntry.getKey() + " on missing key" + describe(joinImplementation, joinType),
                        runOutput.finalState(),
                        equalTo(testEntry.getValue())
                    );
                }
            }
        }
    }

    /*
    ---------------------------------------------------------------------------------------
    Tests of task validation (as done by Kestra when a flow is saved)
    ---------------------------------------------------------------------------------------
    */

    /**
     * Tests that the base task used by validation tests is valid,
     * so that violations found by other validation tests come from the property they change.
     */
    @Test
    void validTaskHasNoViolation() {
        Join task = taskBuilder(defaultKey, defaultKey).build();

        assertThat("Task should be valid", modelValidator.isValid(task).isEmpty(), is(true));
    }

    /**
     * Tests that identical left and right labels are rejected.
     */
    @Test
    void identicalLabelsAreRejected() {
        Join task = taskBuilder(defaultKey, defaultKey)
            .leftLabel("same")
            .rightLabel("same")
            .build();

        var violation = modelValidator.isValid(task);

        assertThat("Identical labels should be rejected", violation.isPresent(), is(true));
        assertThat(violation.get().getMessage(), containsString("leftLabel and rightLabel must be different"));
    }

    /**
     * Tests that a blank label is rejected.
     */
    @Test
    void blankLabelIsRejected() {
        Join task = taskBuilder(defaultKey, defaultKey)
            .leftLabel("   ")
            .build();

        var violation = modelValidator.isValid(task);

        assertThat("Blank label should be rejected", violation.isPresent(), is(true));
        assertThat(violation.get().getMessage(), containsString("leftLabel"));
    }

    /*
    ---------------------------------------------------------------------------------------
    Tests of behaviour with inconsistent inputs
    ---------------------------------------------------------------------------------------
    */

    /**
     * Tests that task fails if key field is missing in one or both data input(s).
     */
    @Test
    void missingKeyFailsByDefault() {

        String leftContent = """
            {id:1,leftValue:"L1"}
            {id:null,leftValue:"L2-A"}
            {id:2,leftValue:"L2-B"}
            {id:3,leftValue:"L3"}
            {id:5,leftValue:"L5"}""";

        String rightContent = """
            {rightValue:"R2-A"}
            {id:2,rightValue:"R2-B"}
            {id:3,rightValue:"R3"}
            {id:4,rightValue:"R4"}
            {id:5,rightValue:"R5-A"}
            {id:5,rightValue:"R5-B"}""";

        Map<String, String[]> contentsBySide =
            Map.of(
                "LEFT", new String[] { leftContent, defaultRightContent },
                "RIGHT", new String[] { defaultLeftContent, rightContent },
                "BOTH", new String[] { leftContent, rightContent }
            );

        // The whole message must match: it gives the row number, never the row contents.
        String expectedErrorPattern = String.format(JoinMessages.MISSING_KEY.replace("%d", "\\d+"), "id", "(LEFT|RIGHT)");

        for (var testEntry : contentsBySide.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                for (JoinType joinType : JoinType.values()) {
                    Exception exception = assertThrows(Exception.class,
                        () -> getOutput(runContext, joinImplementation, joinType));

                    assertThat("Should show specific error for " + testEntry.getKey() + " missing key" + describe(joinImplementation, joinType),
                        exception.getMessage(), matchesPattern(expectedErrorPattern));
                }
            }
        }
    }

    /**
     * Tests invalid types on key fields of data.
     * Should throw a specific error.
     */
    @Test
    void invalidTypeThrowError() {

        String leftContent = """
            {id:{{OiBTIKUgTyAASb8=}},leftValue:"L1"}
            {id:2,leftValue:"L2-A"}
            {id:2,leftValue:"L2-B"}
            {id:3,leftValue:"L3"}
            {id:5,leftValue:"L5"}""";

        String rightContent = """
            {id:2,rightValue:"R2-A"}
            {id:[80, 85, 90],rightValue:"R2-B"}
            {id:3,rightValue:"R3"}
            {id:4,rightValue:"R4"}
            {id:5,rightValue:"R5-A"}
            {id:5,rightValue:"R5-B"}""";

        Map<String, String[]> contentsBySide =
            Map.of(
                "LEFT", new String[] { leftContent, defaultRightContent },
                "RIGHT", new String[] { defaultLeftContent, rightContent }
            );

        for (var testEntry : contentsBySide.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);
            String expectedError = String.format(JoinMessages.UNPROCESSABLE_KEY_TYPE, "id", testEntry.getKey());

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                for (JoinType joinType : JoinType.values()) {
                    Exception exception = assertThrows(Exception.class,
                        () -> getOutput(runContext, joinImplementation, joinType));

                    assertThat("Should show specific error for invalid type on " + testEntry.getKey() + describe(joinImplementation, joinType),
                        exception.getMessage(), equalTo(expectedError));
                }
            }
        }
    }

    /**
     * Tests different types on key fields of same data.
     * Only sorted merge implementation must fail because of impossible sort.
     */
    @Test
    void differentTypesFromSameSideBreaksSortedMergeOnly() {

        String leftContent = """
            {id:1,leftValue:"L1"}
            {id:true,leftValue:"L2-A"}
            {id:true,leftValue:"L2-B"}
            {id:3,leftValue:"L3"}
            {id:5,leftValue:"L5"}""";

        String rightContent = """
            {id:true,rightValue:"R2-A"}
            {id:true,rightValue:"R2-B"}
            {id:3,rightValue:"R3"}
            {id:4,rightValue:"R4"}
            {id:5,rightValue:"R5-A"}
            {id:5,rightValue:"R5-B"}""";

        Map<String, String[]> contentsBySide =
            Map.of(
                "LEFT", new String[] { leftContent, defaultRightContent },
                "RIGHT", new String[] { defaultLeftContent, rightContent }
            );

        // In both cases, the first key read is an INT of the left input.
        String expectedError = String.format(JoinMessages.DIFFERENT_TYPES, "INT", "BOOL");

        for (var testEntry : contentsBySide.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                for (JoinType joinType : JoinType.values()) {
                    String testCase = " on " + testEntry.getKey() + describe(joinImplementation, joinType);

                    if (joinImplementation == JoinImplementation.SORTED_MERGE) {
                        Exception exception = assertThrows(Exception.class,
                            () -> getOutput(runContext, joinImplementation, joinType));

                        assertThat("Should show specific error" + testCase,
                            exception.getMessage(), equalTo(expectedError));
                    } else {
                        assertDoesNotThrow(
                            () -> getOutput(runContext, joinImplementation, joinType),
                            "Should not fail for other implementations than sorted merge" + testCase
                        );
                    }
                }
            }
        }
    }

    /**
     * Tests different types on key fields between data.
     * Only sorted merge implementation must fail because of impossible join.
     */
    @Test
    void differentTypesBetweenSidesBreaksSortedMergeOnly() {

        String rightContent = """
            {id:2.0,rightValue:"R2-A"}
            {id:2.0,rightValue:"R2-B"}
            {id:3.5,rightValue:"R3"}
            {id:4.2,rightValue:"R4"}
            {id:5.1,rightValue:"R5-A"}
            {id:5.0,rightValue:"R5-B"}""";

        String expectedError = String.format(JoinMessages.DIFFERENT_TYPES, "INT", "DECIMAL");

        RunContext runContext = getRunContext(defaultLeftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                if (joinImplementation == JoinImplementation.SORTED_MERGE) {
                    Exception exception = assertThrows(Exception.class,
                        () -> getOutput(runContext, joinImplementation, joinType));

                    assertThat("Should show specific error" + describe(joinImplementation, joinType),
                        exception.getMessage(), equalTo(expectedError));
                } else {
                    assertDoesNotThrow(
                        () -> getOutput(runContext, joinImplementation, joinType),
                        "Should not fail for other implementations than sorted merge" + describe(joinImplementation, joinType)
                    );
                }
            }
        }
    }

    /**
     * Test of nested keys (with .) whose parent value exists but is not an object.
     * Should throw a specific error.
     */
    @Test
    void unreachableKeyDepthThrowError() {

        Map<String, String[]> testKeysBySide =
            Map.of(
                "LEFT", new String[] { "id.x", defaultKey },
                "RIGHT", new String[] { defaultKey, "id.x" }
            );

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                for (var testEntry : testKeysBySide.entrySet()) {
                    String expectedError = String.format(JoinMessages.UNREACHABLE_KEY_DEPTH, "id.x", testEntry.getKey());

                    Exception exception = assertThrows(Exception.class,
                        () -> getOutput(runContext, joinImplementation, joinType, testEntry.getValue()[0], testEntry.getValue()[1]));

                    assertThat("Should show specific error for " + testEntry.getKey() + " invalid key" + describe(joinImplementation, joinType),
                        exception.getMessage(), equalTo(expectedError));
                }
            }
        }
    }

    /**
     * Tests that a row whose parent object of a nested key is absent or null is handled as a missing key,
     * according to the onMissingKey option, instead of failing as an unreachable key depth.
     */
    @Test
    void missingNestedKeyParentFollowsOnMissingKey() throws Exception {

        String leftContent = """
            {customer:{id:1},leftValue:"L1"}
            {leftValue:"L2"}
            {customer:null,leftValue:"L3"}""";

        String rightContent = """
            {customer:{id:1},rightValue:"R1"}""";

        String nestedKey = "customer.id";

        String expected = """
            {customer:{id:1},leftValue:"L1",rightValue:"R1"}""";

        // The first missing key is at the second row of the left input.
        String expectedError = String.format(JoinMessages.MISSING_KEY, nestedKey, "LEFT", 2);

        Map<MissingKeyAction, Optional<State.Type>> expectedStates =
            Map.of(
                MissingKeyAction.WARNING, Optional.of(State.Type.WARNING),
                MissingKeyAction.IGNORE, Optional.empty()
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Exception exception = assertThrows(Exception.class,
                () -> getOutput(runContext, joinImplementation, JoinType.INNER, nestedKey, nestedKey, MissingKeyAction.FAIL));

            assertThat("Missing parent should fail as a missing key with FAIL" + describe(joinImplementation, JoinType.INNER),
                exception.getMessage(), equalTo(expectedError));

            for (var testEntry : expectedStates.entrySet()) {
                Join.Output runOutput = getOutput(runContext, joinImplementation, JoinType.INNER, nestedKey, nestedKey, testEntry.getKey());

                assertThat("Final state with " + testEntry.getKey() + " on missing parent" + describe(joinImplementation, JoinType.INNER),
                    runOutput.finalState(), equalTo(testEntry.getValue()));
                assertThat("Rows with a missing parent should not match with " + testEntry.getKey() + describe(joinImplementation, JoinType.INNER),
                    sortIonLines(readOutput(runOutput)), equalTo(sortIonLines(expected)));
            }
        }
    }

    /**
     * Tests the case where key match an object in data instead of a raw value.
     * Should throw a specific error.
     */
    @Test
    void objectKeyThrowError() {

        String leftContent = """
            {object:{id:1},leftValue:"L1"}
            {object:{id:2},leftValue:"L2-A"}
            {object:{id:2},leftValue:"L2-B"}
            {object:{id:3},leftValue:"L3"}
            {object:{id:5},leftValue:"L5"}""";

        String rightContent = """
            {object:{id:2},rightValue:"R2-A"}
            {object:{id:2},rightValue:"R2-B"}
            {object:{id:3},rightValue:"R3"}
            {object:{id:4},rightValue:"R4"}
            {object:{id:5},rightValue:"R5-A"}
            {object:{id:5},rightValue:"R5-B"}""";

        Map<String, String[]> testKeysBySide =
            Map.of(
                "LEFT", new String[] { "object", "object.id" },
                "RIGHT", new String[] { "object.id", "object" }
            );

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : JoinType.values()) {
                for (var testEntry : testKeysBySide.entrySet()) {
                    String expectedError = String.format(JoinMessages.KEY_VALUE_IS_OBJECT, "object", testEntry.getKey());

                    Exception exception = assertThrows(Exception.class,
                        () -> getOutput(runContext, joinImplementation, joinType, testEntry.getValue()[0], testEntry.getValue()[1]));

                    assertThat("Should show specific error for " + testEntry.getKey() + " invalid key" + describe(joinImplementation, joinType),
                        exception.getMessage(), equalTo(expectedError));
                }
            }
        }
    }

    /**
     * Tests that a stored input whose top-level values are not structs fails with a message naming the side and the found type.
     * The message is searched in the whole cause chain, as the error may be wrapped by a read error.
     */
    @Test
    void nonStructRowThrowError() {

        Map<String, String[]> contentsBySide =
            Map.of(
                "LEFT", new String[] { "[{id:1}]", defaultRightContent, "LIST" },
                "RIGHT", new String[] { defaultLeftContent, "42", "INT" }
            );

        for (var testEntry : contentsBySide.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);
            String expectedError = String.format(IterableIonReader.NOT_A_STRUCT, testEntry.getKey(), testEntry.getValue()[2]);

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                Exception exception = assertThrows(Exception.class,
                    () -> getOutput(runContext, joinImplementation, JoinType.INNER));

                StringBuilder messages = new StringBuilder();
                for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                    messages.append(cause.getMessage()).append('\n');
                }

                assertThat("Should show specific error for " + testEntry.getKey() + " non-struct row" + describe(joinImplementation, JoinType.INNER),
                    messages.toString(), containsString(expectedError));
            }
        }
    }

    /*
    ---------------------------------------------------------------------------------------
    Tests of integration in the transform plugin (merge mode, in-memory records, output type and format)
    ---------------------------------------------------------------------------------------
    */

    /**
     * Builder of a valid task joining in-memory records on the default key.
     * @param left Left records.
     * @param right Right records.
     * @return Task builder with id, type, inputs and keys.
     */
    private Join.JoinBuilder<?, ?> inMemoryTaskBuilder(List<Map<String, Object>> left, List<Map<String, Object>> right) {
        return Join.builder()
            .id(IdUtils.create())
            .type(Join.class.getName())
            .left(Property.ofValue(left))
            .right(Property.ofValue(right))
            .leftKey(Property.ofValue(defaultKey))
            .rightKey(Property.ofValue(defaultKey));
    }

    /**
     * Read the output file of a task run as Ion values, whatever its format (text or binary).
     * @param output Output of the task run.
     * @return One line per value, in compact Ion text.
     * @throws IOException If the file cannot be read from storage.
     */
    private String readOutputValues(Join.Output output) throws IOException {
        try (InputStream stream = this.storageInterface.get(tenant, null, output.getUri())) {
            Iterator<IonValue> values = IonValueUtils.system().iterate(stream);
            StringBuilder lines = new StringBuilder();
            while (values.hasNext()) {
                lines.append(values.next()).append('\n');
            }
            return lines.toString();
        }
    }

    /**
     * Tests that with the default OVERWRITE merge mode, the right value is kept on a field name conflict,
     * and that the field keeps its position.
     */
    @Test
    void overwriteKeepsRightValue() throws Exception {

        String leftContent = """
            {id:1,value:"L1",leftOnly:"A"}""";

        String rightContent = """
            {id:1,value:"R1",rightOnly:"B"}""";

        String expected = """
            {id:1,value:"R1",leftOnly:"A",rightOnly:"B"}""";

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Join.Output runOutput = getOutput(runContext, joinImplementation, JoinType.INNER);

            assertThat("Right value should overwrite left value" + describe(joinImplementation, JoinType.INNER),
                sortIonLines(readOutput(runOutput)), equalTo(expected));
        }
    }

    /**
     * Tests that in-memory records are joined and, with the default AUTO output type, returned as records.
     */
    @Test
    void inMemoryRecordsReturnRecords() throws Exception {

        List<Map<String, Object>> left = List.of(
            Map.of("id", 1, "leftValue", "L1"),
            Map.of("id", 2, "leftValue", "L2")
        );
        List<Map<String, Object>> right = List.of(
            Map.of("id", 2, "rightValue", "R2"),
            Map.of("id", 3, "rightValue", "R3")
        );

        RunContext runContext = this.runContextFactory.of(Map.of());

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Join task = inMemoryTaskBuilder(left, right)
                .joinImplementation(Property.ofValue(joinImplementation))
                .build();

            Join.Output runOutput = task.run(runContext);
            String testCase = describe(joinImplementation, JoinType.INNER);

            assertThat("No file should be stored" + testCase, runOutput.getUri(), equalTo(null));
            assertThat("Records should be joined" + testCase, runOutput.getRecords(),
                equalTo(List.of(Map.of("id", 2L, "leftValue", "L2", "rightValue", "R2"))));
            assertThat("Output count should be correct" + testCase, runOutput.getOutputSize(), equalTo(1L));
            assertThat("Left count should be correct" + testCase, runOutput.getLeftSize(), equalTo(2L));
            assertThat("Right count should be correct" + testCase, runOutput.getRightSize(), equalTo(2L));
        }
    }

    /**
     * Tests that the output type forces records for stored inputs, and a stored file for in-memory inputs.
     */
    @Test
    void outputTypeIsForced() throws Exception {

        String expected = sortIonLines(defaultExpectedResults.get(JoinType.INNER));
        long expectedCount = expected.split("\n").length;

        RunContext storedRunContext = getRunContext(defaultLeftContent, defaultRightContent);

        List<Map<String, Object>> left = List.of(Map.of("id", 1, "leftValue", "L1"));
        List<Map<String, Object>> right = List.of(Map.of("id", 1, "rightValue", "R1"));

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            String testCase = describe(joinImplementation, JoinType.INNER);

            Join.Output recordsOutput = taskBuilder(defaultKey, defaultKey)
                .joinImplementation(Property.ofValue(joinImplementation))
                .outputType(Property.ofValue(Join.OutputMode.RECORDS))
                .build()
                .run(storedRunContext);

            assertThat("No file should be stored with RECORDS" + testCase, recordsOutput.getUri(), equalTo(null));
            assertThat("Records should be returned with RECORDS" + testCase,
                (long) recordsOutput.getRecords().size(), equalTo(expectedCount));

            Join.Output storeOutput = inMemoryTaskBuilder(left, right)
                .joinImplementation(Property.ofValue(joinImplementation))
                .outputType(Property.ofValue(Join.OutputMode.STORE))
                .build()
                .run(this.runContextFactory.of(Map.of()));

            assertThat("No records should be returned with STORE" + testCase, storeOutput.getRecords(), equalTo(null));
            // Compared as Ion values (field order ignored): Map.of() gives no order to the input fields.
            assertThat("A file should be stored with STORE" + testCase,
                IonValueUtils.system().singleValue(readOutput(storeOutput)),
                equalTo(IonValueUtils.system().singleValue("{id:1,leftValue:\"L1\",rightValue:\"R1\"}")));
        }
    }

    /**
     * Tests that a binary output file contains the same values as a text one.
     */
    @Test
    void binaryOutputIsReadable() throws Exception {

        String expected = sortIonLines(defaultExpectedResults.get(JoinType.INNER));

        RunContext runContext = getRunContext(defaultLeftContent, defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Join.Output runOutput = taskBuilder(defaultKey, defaultKey)
                .joinImplementation(Property.ofValue(joinImplementation))
                .outputFormat(Property.ofValue(OutputFormat.BINARY))
                .build()
                .run(runContext);

            assertThat("Binary output should contain the joined values" + describe(joinImplementation, JoinType.INNER),
                sortIonLines(readOutputValues(runOutput)), equalTo(expected));
        }
    }

    /**
     * Tests that with an empty left input, right rows are written once.
     * Regression test: nested loop used to write them twice for right outer, full outer and right anti joins.
     */
    @Test
    void emptyLeftDoesNotDuplicateRightRows() throws Exception {

        String expected = sortIonLines(defaultRightContent);
        JoinType[] joinTypes = { JoinType.FULL_OUTER, JoinType.RIGHT_OUTER, JoinType.RIGHT_ANTI };

        RunContext runContext = getRunContext("", defaultRightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            for (JoinType joinType : joinTypes) {
                Join.Output runOutput = getOutput(runContext, joinImplementation, joinType);

                assertThat("Right rows should be written once" + describe(joinImplementation, joinType),
                    sortIonLines(readOutput(runOutput)), equalTo(expected));
            }
        }
    }

    /**
     * Tests that integer keys beyond the int range are compared exactly.
     * Regression test: sorted merge used to truncate them, so that 4294967297 was equal to 1.
     */
    @Test
    void bigIntegerKeysAreNotTruncated() throws Exception {

        String leftContent = """
            {id:4294967297,leftValue:"L-big"}
            {id:1,leftValue:"L1"}""";

        String rightContent = """
            {id:1,rightValue:"R1"}
            {id:4294967297,rightValue:"R-big"}""";

        String expected = sortIonLines("""
            {id:1,leftValue:"L1",rightValue:"R1"}
            {id:4294967297,leftValue:"L-big",rightValue:"R-big"}""");

        RunContext runContext = getRunContext(leftContent, rightContent);

        for (JoinImplementation joinImplementation : JoinImplementation.values()) {
            Join.Output runOutput = getOutput(runContext, joinImplementation, JoinType.INNER);

            assertThat("Big integer keys should only match equal keys" + describe(joinImplementation, JoinType.INNER),
                sortIonLines(readOutput(runOutput)), equalTo(expected));
        }
    }

    /**
     * Tests that every implementation matches decimal keys by value and timestamp keys by instant.
     * Regression test: hash and nested loop joins used to compare keys with IonValue.equals,
     * which depends on decimal precision and timestamp offset, unlike sorted merge.
     * With the default OVERWRITE merge mode, the output keeps the right key.
     */
    @Test
    void decimalAndTimestampKeysMatchInEveryImplementation() throws Exception {

        Map<String, String[]> contentsByKeyType =
            Map.of(
                "decimal", new String[] {
                    """
                    {id:1.0,leftValue:"L1"}
                    {id:2.50,leftValue:"L2"}""",
                    """
                    {id:1.00,rightValue:"R1"}
                    {id:2.5,rightValue:"R2"}""",
                    """
                    {id:1.00,leftValue:"L1",rightValue:"R1"}
                    {id:2.5,leftValue:"L2",rightValue:"R2"}"""
                },
                "timestamp", new String[] {
                    """
                    {id:2024-01-01T00:00Z,leftValue:"L1"}
                    {id:2024-01-02T,leftValue:"L2"}""",
                    """
                    {id:2024-01-01T01:00+01:00,rightValue:"R1"}
                    {id:2024-01-02T00:00:00.000Z,rightValue:"R2"}""",
                    """
                    {id:2024-01-01T01:00+01:00,leftValue:"L1",rightValue:"R1"}
                    {id:2024-01-02T00:00:00.000Z,leftValue:"L2",rightValue:"R2"}"""
                }
            );

        for (var testEntry : contentsByKeyType.entrySet()) {
            RunContext runContext = getRunContext(testEntry.getValue()[0], testEntry.getValue()[1]);
            String expected = sortIonLines(testEntry.getValue()[2]);

            for (JoinImplementation joinImplementation : JoinImplementation.values()) {
                Join.Output runOutput = getOutput(runContext, joinImplementation, JoinType.INNER);

                assertThat("Equal " + testEntry.getKey() + " keys should match" + describe(joinImplementation, JoinType.INNER),
                    sortIonLines(readOutput(runOutput)), equalTo(expected));
            }
        }
    }
}
