# AdapQuant
# AdapQuant

**AdapQuant** is a resource-adaptive quantile sketch designed for continuous data streams on small or resource-changing devices. It allows the currently active quantile sketch to change according to the available device-resource state while preserving useful summaries from earlier periods.

Instead of repeatedly resizing the complete historical sketch, AdapQuant separates the current stream from completed resource epochs. Earlier summaries are retained in a bounded historical archive and are combined temporarily when a complete-history quantile query is required.

## Main Features

- Resource-state-driven quantile sketch configuration
- Four resource states: `HIGH`, `MEDIUM`, `LOW`, and `CRITICAL`
- Epoch-based historical preservation
- Bounded archive of completed summaries
- Controlled consolidation of older summaries
- Historical reconstruction for complete-stream quantile queries
- Separate reporting of active sketch, archive, and query-workspace sizes
- Java 17 implementation
- Apache DataSketches KLL used as the underlying quantile summary

## Default Configuration

| Resource State | Active K |
|---|---:|
| HIGH | 400 |
| MEDIUM | 200 |
| LOW | 100 |
| CRITICAL | 100 |

Additional parameters:

| Parameter | Value |
|---|---:|
| Maximum archived summaries | 3 |
| Historical consolidation K | 300 |
| Historical query K | 400 |

These are the parameters used in the experimental evaluation of AdapQuant.

## Requirements

AdapQuant requires:

- Java 17
- Apache DataSketches Java 7.0.1
- Apache DataSketches Memory 4.1.0

Required JAR files:

```text
AdapQuant.jar
datasketches-java-7.0.1.jar
datasketches-memory-4.1.0.jar
```

When using Java 17 with the DataSketches Memory library, the following JVM option may also be required:

```text
--add-modules jdk.incubator.foreign
```

## Basic Usage

The following example creates an AdapQuant sketch in the `HIGH` resource state, processes observations, moves temporarily to a constrained state, and later returns to the `HIGH` state.

```java
public class Example {

    public static void main(String[] args) {

        AdapQuant sketch =
                new AdapQuant(AdapQuant.ResourceState.HIGH);

        // High-resource period
        for (int i = 1; i <= 10000; i++) {
            sketch.update(i);
        }

        // Device becomes resource constrained
        sketch.setResourceState(
                AdapQuant.ResourceState.LOW);

        for (int i = 10001; i <= 20000; i++) {
            sketch.update(i);
        }

        // Resources recover
        sketch.setResourceState(
                AdapQuant.ResourceState.HIGH);

        for (int i = 20001; i <= 30000; i++) {
            sketch.update(i);
        }

        double[] quantiles =
                sketch.getQuantiles(0.50, 0.90, 0.99);

        System.out.printf(
                "P50=%.2f P90=%.2f P99=%.2f%n",
                quantiles[0],
                quantiles[1],
                quantiles[2]);
    }
}
```

## Resource-State Changes

A resource-state transition is made explicitly using:

```java
sketch.setResourceState(
        AdapQuant.ResourceState.MEDIUM);
```

Available states are:

```java
AdapQuant.ResourceState.HIGH
AdapQuant.ResourceState.MEDIUM
AdapQuant.ResourceState.LOW
AdapQuant.ResourceState.CRITICAL
```

The current state can be obtained using:

```java
AdapQuant.ResourceState state =
        sketch.getResourceState();
```

The currently active K value can be obtained with:

```java
int k = sketch.getActiveK();
```

## Adding Stream Observations

Each numerical stream observation is inserted using:

```java
sketch.update(value);
```

For example:

```java
sketch.update(42.7);
```

The total number of processed observations is available through:

```java
long n = sketch.getCount();
```

## Quantile Queries

A single quantile can be requested using:

```java
double median = sketch.getQuantile(0.50);
```

Multiple quantiles can be queried together:

```java
double[] q =
        sketch.getQuantiles(0.50, 0.90, 0.99);
```

In AdapQuant, a complete-history query temporarily reconstructs the historical view by combining archived summaries with the currently active sketch.

The temporary reconstruction sketch is discarded after the query.

## Memory Reporting

AdapQuant deliberately separates memory-related quantities because the active ingestion sketch, persistent historical archive, and temporary query workspace have different roles.

### Active Sketch Size

```java
int activeBytes =
        sketch.getActiveSizeBytes();
```

This reports the serialized size of the currently active sketch.

### Historical Archive Size

```java
long archiveBytes =
        sketch.getArchiveSizeBytes();
```

This reports the total serialized size of the stored historical summaries.

### Query Workspace Size

```java
int queryBytes =
        sketch.getQueryWorkspaceSizeBytes();
```

This reports the size of the temporary reconstruction sketch used for complete-history queries.

### Persistent Storage

```java
long persistentBytes =
        sketch.getPersistentSizeBytes();
```

This represents:

```text
active sketch + historical archive
```

The query workspace is temporary and is therefore not included in persistent storage.

## Archive Information

The current number of historical summaries can be obtained using:

```java
int archived =
        sketch.getArchivedSummaryCount();
```

The implementation also exposes:

```java
long transitions =
        sketch.getTransitionCount();

long consolidations =
        sketch.getConsolidationCount();
```

These values can be useful when evaluating resource-state behavior.

## Public API Summary

| Method | Purpose |
|---|---|
| `AdapQuant()` | Creates an AdapQuant instance using the default resource state |
| `AdapQuant(ResourceState)` | Creates an instance in a specified state |
| `update(double)` | Adds one stream observation |
| `setResourceState(ResourceState)` | Changes the current resource state |
| `getQuantile(double)` | Returns one approximate quantile |
| `getQuantiles(double...)` | Returns multiple approximate quantiles |
| `getResourceState()` | Returns the current resource state |
| `getActiveK()` | Returns the current active K |
| `getCount()` | Returns the number of processed observations |
| `getActiveSizeBytes()` | Returns active serialized sketch size |
| `getArchiveSizeBytes()` | Returns persistent archive size |
| `getQueryWorkspaceSizeBytes()` | Returns temporary query workspace size |
| `getPersistentSizeBytes()` | Returns active + archive storage |
| `getArchivedSummaryCount()` | Returns the number of archived summaries |
| `getTransitionCount()` | Returns the number of resource-state transitions |
| `getConsolidationCount()` | Returns the number of archive consolidations |

## Compiling a Program with AdapQuant

On Windows:

```text
javac -cp "AdapQuant.jar;datasketches-java-7.0.1.jar;datasketches-memory-4.1.0.jar" Example.java
```

Run it using:

```text
java --add-modules jdk.incubator.foreign -cp "AdapQuant.jar;datasketches-java-7.0.1.jar;datasketches-memory-4.1.0.jar;." Example
```

On Linux or macOS, replace semicolons (`;`) in the classpath with colons (`:`).

## Design Notes

AdapQuant does not directly resize the complete historical sketch whenever available resources change.

When a resource-state transition occurs:

1. the current active sketch is closed;
2. its summary is preserved as a completed epoch;
3. a new active sketch is created for the new resource state;
4. older summaries are consolidated when the archive limit is exceeded.

The maximum number of archived summaries is three. When this limit is exceeded, the two oldest summaries are merged using a KLL sketch with `k = 300`.

Complete-history quantile queries are reconstructed using a temporary KLL sketch with `k = 400`.

## Intended Use

AdapQuant is intended for research and experimentation involving:

- resource-constrained stream processing;
- IoT and edge devices;
- embedded systems;
- mobile or intermittently constrained devices;
- adaptive streaming analytics;
- approximate quantile monitoring.

The current implementation assumes that the application or an external resource monitor determines the device resource state and calls `setResourceState(...)`.

Automatic operating-system-level resource-state detection is not included in the current library.

## Limitations

AdapQuant is not designed to achieve the lowest possible quantile error under a permanently fixed high-memory budget.

Experimental evaluation showed that:

- fixed KLL can provide better accuracy in some conditions;
- SplineSketch provides substantially higher quantile accuracy;
- prolonged low-resource operation can increase AdapQuant's error;
- historical preservation requires additional persistent storage;
- historical reconstruction introduces additional query cost.

The purpose of AdapQuant is instead to combine changing active resource usage with explicit and bounded preservation of historical stream information.

## Reproducibility

The research evaluation uses separate experimental software containing the workloads, resource-state schedules, baselines, repetitions, and CSV-generation procedures.

The reusable `AdapQuant.jar` contains the core AdapQuant implementation and should not be confused with the complete experimental evaluation package.

Because KLL uses randomized compaction internally, repeated executions may not always produce byte-identical sketch states or exactly identical quantile estimates. Experimental reproducibility should therefore be interpreted in terms of the defined procedure, configuration, and statistically comparable results.

## Citation

If you use AdapQuant in academic work, please cite the associated paper once the final publication information becomes available.

A BibTeX entry can be added here after publication:

```bibtex
@article{adapquant,
  title   = {AdapQuant: A Resource-Adaptive Quantile Sketch with Historical Preservation for Small Devices},
  author  = {...},
  journal = {...},
  year    = {...}
}
```

## Third-Party Dependency

AdapQuant uses Apache DataSketches KLL as its underlying quantile-summary implementation.

Apache DataSketches is a separate third-party project and is distributed under its own license. Users should retain and comply with the applicable Apache DataSketches licensing terms when distributing or using its libraries.
