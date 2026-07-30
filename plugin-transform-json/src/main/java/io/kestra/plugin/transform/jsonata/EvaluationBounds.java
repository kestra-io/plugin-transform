package io.kestra.plugin.transform.jsonata;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;

/**
 * Depth and timeout guard for a single JSONata evaluation.
 *
 * <p>Replaces {@link Jsonata.Frame#setRuntimeBounds(long, int)}, whose {@code Timebox} skips the
 * increment and the decrement whenever the frame carries {@code isParallelCall}. Entry and exit do
 * not observe the same flag for a given {@code evaluate()} call, so the counter gains one per input
 * item and never unwinds — making {@code maxDepth} bound input size instead of recursion depth
 * (kestra-io/plugin-transform#102). Counting unconditionally keeps the two sides symmetric, which is
 * safe because the Java port evaluates path steps sequentially.
 */
final class EvaluationBounds {

    private final int maxDepth;
    private final long timeoutMillis;
    private final long startedAt;

    private int depth;

    static void register(Jsonata.Frame frame, int maxDepth, long timeoutMillis) {
        var bounds = new EvaluationBounds(maxDepth, timeoutMillis);
        frame.setEvaluateEntryCallback((expression, input, environment) -> bounds.enter());
        frame.setEvaluateExitCallback((expression, input, environment, result) -> bounds.exit());
    }

    private EvaluationBounds(int maxDepth, long timeoutMillis) {
        this.maxDepth = maxDepth;
        this.timeoutMillis = timeoutMillis;
        this.startedAt = System.currentTimeMillis();
    }

    private void enter() {
        if (++depth > maxDepth) {
            throw new JException(
                "JSONata expression exceeded maxDepth=" + maxDepth + " nested evaluation levels. "
                    + "Raise maxDepth if the expression legitimately recurses that deep, otherwise check "
                    + "for a recursive function that never reaches its terminating case.",
                -1
            );
        }

        if (System.currentTimeMillis() - startedAt > timeoutMillis) {
            throw new JException(
                "JSONata evaluation exceeded timeout=" + timeoutMillis + "ms. "
                    + "Raise timeout for genuinely long transformations, otherwise check for a "
                    + "non-terminating expression.",
                -1
            );
        }
    }

    private void exit() {
        depth--;
    }
}
