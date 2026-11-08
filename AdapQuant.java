import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.datasketches.kll.KllDoublesSketch;

/**
 * AdapQuant: a resource-adaptive quantile sketch with bounded historical
 * preservation for resource-changing small devices.
 *
 * <p>The implementation uses Apache DataSketches KLL as its underlying
 * quantile summary. When the resource state changes, the current active
 * sketch is serialized and preserved as a completed epoch. A new active
 * sketch is then created using the K value associated with the new state.</p>
 *
 * <p>Historical storage is bounded to three archived summaries. If the
 * archive exceeds this limit, the two oldest summaries are consolidated
 * into a KLL sketch with k=300. Complete-history queries are reconstructed
 * temporarily using a KLL sketch with k=400.</p>
 *
 * <p>Required runtime dependencies:</p>
 * <ul>
 *   <li>Apache DataSketches Java 7.0.1</li>
 *   <li>Apache DataSketches Memory 4.1.0</li>
 * </ul>
 */
public final class AdapQuant {

    public static final int MAX_ARCHIVED_SUMMARIES = 3;
    public static final int CONSOLIDATION_K = 300;
    public static final int QUERY_K = 400;

    /** Resource states and their corresponding active K values. */
    public enum ResourceState {
        HIGH(400),
        MEDIUM(200),
        LOW(100),
        CRITICAL(100);

        private final int k;

        ResourceState(int k) {
            this.k = k;
        }

        public int getK() {
            return k;
        }
    }

    private ResourceState state;
    private KllDoublesSketch active;
    private final List<byte[]> archive = new ArrayList<>();

    private long count;
    private long transitionCount;
    private long consolidationCount;

    /** Creates a new AdapQuant instance in the HIGH resource state. */
    public AdapQuant() {
        this(ResourceState.HIGH);
    }

    /** Creates a new AdapQuant instance in the specified resource state. */
    public AdapQuant(ResourceState initialState) {
        this.state = Objects.requireNonNull(initialState, "initialState");
        this.active = KllDoublesSketch.newHeapInstance(initialState.getK());
    }

    /** Adds one numerical observation to the currently active sketch. */
    public synchronized void update(double value) {
        active.update(value);
        count++;
    }

    /**
     * Changes the active resource state.
     *
     * <p>If the state actually changes, the current active sketch is archived
     * as a completed epoch. The archive is consolidated when necessary, and
     * a new active sketch is created for the new resource state.</p>
     */
    public synchronized void setResourceState(ResourceState newState) {
        Objects.requireNonNull(newState, "newState");

        if (newState == state) {
            return;
        }

        archive.add(active.toByteArray());
        transitionCount++;

        while (archive.size() > MAX_ARCHIVED_SUMMARIES) {
            consolidateOldestTwo();
        }

        state = newState;
        active = KllDoublesSketch.newHeapInstance(newState.getK());
    }

    /** Returns an approximate complete-history quantile for q in [0,1]. */
    public synchronized double getQuantile(double q) {
        if (q < 0.0 || q > 1.0 || Double.isNaN(q)) {
            throw new IllegalArgumentException("q must be in [0,1]");
        }

        KllDoublesSketch querySketch = buildQuerySketch();
        return querySketch.getQuantile(q);
    }

    /** Returns approximate complete-history quantiles for the supplied ranks. */
    public synchronized double[] getQuantiles(double... ranks) {
        Objects.requireNonNull(ranks, "ranks");

        KllDoublesSketch querySketch = buildQuerySketch();
        double[] result = new double[ranks.length];

        for (int i = 0; i < ranks.length; i++) {
            double q = ranks[i];
            if (q < 0.0 || q > 1.0 || Double.isNaN(q)) {
                throw new IllegalArgumentException("all ranks must be in [0,1]");
            }
            result[i] = querySketch.getQuantile(q);
        }

        return result;
    }

    /** Returns the serialized size, in bytes, of the current active sketch. */
    public synchronized int getActiveSizeBytes() {
        return active.toByteArray().length;
    }

    /** Returns the total serialized size, in bytes, of archived summaries. */
    public synchronized long getArchiveSizeBytes() {
        long total = 0L;
        for (byte[] bytes : archive) {
            total += bytes.length;
        }
        return total;
    }

    /**
     * Returns the serialized size, in bytes, of a temporary complete-history
     * query sketch reconstructed from the archive and current active sketch.
     */
    public synchronized int getQueryWorkspaceSizeBytes() {
        return buildQuerySketch().toByteArray().length;
    }

    /** Returns active sketch size plus persistent archive size. */
    public synchronized long getPersistentSizeBytes() {
        return (long) getActiveSizeBytes() + getArchiveSizeBytes();
    }

    public synchronized ResourceState getResourceState() {
        return state;
    }

    public synchronized int getActiveK() {
        return state.getK();
    }

    public synchronized long getCount() {
        return count;
    }

    public synchronized int getArchivedSummaryCount() {
        return archive.size();
    }

    public synchronized long getTransitionCount() {
        return transitionCount;
    }

    public synchronized long getConsolidationCount() {
        return consolidationCount;
    }

    /**
     * Returns defensive copies of the serialized archived summaries.
     * The returned list is unmodifiable.
     */
    public synchronized List<byte[]> getArchivedSummaries() {
        List<byte[]> copy = new ArrayList<>(archive.size());
        for (byte[] bytes : archive) {
            copy.add(bytes.clone());
        }
        return Collections.unmodifiableList(copy);
    }

    private KllDoublesSketch buildQuerySketch() {
        KllDoublesSketch querySketch = KllDoublesSketch.newHeapInstance(QUERY_K);

        for (byte[] bytes : archive) {
            querySketch.merge(heapify(bytes));
        }

        querySketch.merge(active);
        return querySketch;
    }

    private void consolidateOldestTwo() {
        byte[] first = archive.remove(0);
        byte[] second = archive.remove(0);

        KllDoublesSketch consolidated =
                KllDoublesSketch.newHeapInstance(CONSOLIDATION_K);

        consolidated.merge(heapify(first));
        consolidated.merge(heapify(second));

        archive.add(0, consolidated.toByteArray());
        consolidationCount++;
    }

    /**
     * Reconstructs a KLL sketch from its serialized representation.
     * Reflection is used here to remain compatible with the DataSketches
     * Java 7.0.1 / Memory 4.1.0 combination used in the experiments.
     */
    private static KllDoublesSketch heapify(byte[] bytes) {
        try {
            Class<?> memoryClass =
                    Class.forName("org.apache.datasketches.memory.Memory");

            Method wrapMethod = memoryClass.getMethod("wrap", byte[].class);
            Object memory = wrapMethod.invoke(null, (Object) bytes);

            Method heapifyMethod =
                    KllDoublesSketch.class.getMethod("heapify", memoryClass);

            return (KllDoublesSketch) heapifyMethod.invoke(null, memory);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to reconstruct KLL summary. "
                    + "Use DataSketches Java 7.0.1 with Memory 4.1.0.",
                    ex);
        }
    }
}
