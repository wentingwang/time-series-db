/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opensearch.tsdb.query.utils.RamUsageConstants;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;

/**
 * Internal aggregation result for time series pipeline aggregators.
 *
 * <p>This class represents the result of time series pipeline aggregations, containing
 * a collection of time series data that can be processed through various pipeline stages.
 * It implements the {@link TimeSeriesProvider} interface to provide access to the
 * underlying time series data.</p>
 *
 * <h2>Wire Format Versions:</h2>
 * <ul>
 *   <li><strong>V0 (Legacy):</strong> Per-sample serialization, no sentinel byte</li>
 *   <li><strong>V1:</strong> Delta-encoded SampleList, sentinel -1</li>
 *   <li><strong>V2:</strong> V1 + 7 unconditional longs for exec-stats, sentinel -2</li>
 * </ul>
 */
public class InternalTimeSeries extends InternalAggregation implements TimeSeriesProvider {

    private final List<TimeSeries> timeSeries;
    private final UnaryPipelineStage reduceStage;
    private final AggregationExecStats execStats;
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

    public static final int LEGACY_SERIAL_VERSION = 0;
    public static final int CURRENT_SERIAL_VERSION = 2;
    public static final Set<Integer> SUPPORTED_VERSIONS = Set.of(0, 1, 2);

    public static volatile int serialFormatSetting = LEGACY_SERIAL_VERSION; // this will be synced with the cluster setting

    /**
     * Private inner record used to pass deserialized state from static readFromVN methods
     * back to the StreamInput constructor, allowing final field assignment.
     */
    private record DeserializedState(List<TimeSeries> timeSeries, UnaryPipelineStage reduceStage, AggregationExecStats execStats) {
    }

    /**
     * Creates a new InternalTimeSeries aggregation result without a reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeries, Map<String, Object> metadata) {
        this(name, timeSeries, metadata, null, AggregationExecStats.EMPTY);
    }

    /**
     * Creates a new InternalTimeSeries aggregation result with an optional reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeries, Map<String, Object> metadata, UnaryPipelineStage reduceStage) {
        this(name, timeSeries, metadata, reduceStage, AggregationExecStats.EMPTY);
    }

    /**
     * Creates a new InternalTimeSeries aggregation result with an optional reduce stage and execution stats.
     *
     * @param name the name of the aggregation
     * @param timeSeries the list of time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     * @param execStats the execution stats snapshot for this shard result (use {@link AggregationExecStats#EMPTY} when not needed)
     */
    public InternalTimeSeries(
        String name,
        List<TimeSeries> timeSeries,
        Map<String, Object> metadata,
        UnaryPipelineStage reduceStage,
        AggregationExecStats execStats
    ) {
        super(name, metadata);
        this.timeSeries = timeSeries;
        this.reduceStage = reduceStage;
        this.execStats = execStats != null ? execStats : AggregationExecStats.EMPTY;
    }

    /**
     * Reads an InternalTimeSeries from a stream for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        int firstVInt = in.readVInt();
        int serialVersion = resolveSerialVersion(firstVInt);
        DeserializedState state;
        switch (serialVersion) {
            case 0:
                state = readFromV0(in, firstVInt);
                break;
            case 1:
                state = readFromV1(in);
                break;
            case 2:
                state = readFromV2(in);
                break;
            default:
                throw new IllegalStateException("Unsupported serial version: " + serialVersion);
        }
        this.timeSeries = state.timeSeries();
        this.reduceStage = state.reduceStage();
        this.execStats = state.execStats();
    }

    private static int resolveSerialVersion(int firstVInt) {
        if (firstVInt >= 0) {
            return LEGACY_SERIAL_VERSION;
        }
        int version = -firstVInt;
        if (!SUPPORTED_VERSIONS.contains(version)) {
            throw new IllegalStateException("Unknown serial version: " + version + ". Supported versions: " + SUPPORTED_VERSIONS);
        }
        return version;
    }

    // ==================== Version-dispatched deserialization ====================

    /**
     * Reads V0 (legacy) format. The firstVInt is the timeSeriesCount (already read by caller).
     */
    private static DeserializedState readFromV0(StreamInput in, int timeSeriesCount) throws IOException {
        List<TimeSeries> series = new ArrayList<>(timeSeriesCount);
        for (int i = 0; i < timeSeriesCount; i++) {
            series.add(readTimeSeriesLegacy(in));
        }
        UnaryPipelineStage reduceStage = readReduceStage(in);
        return new DeserializedState(series, reduceStage, AggregationExecStats.EMPTY);
    }

    /**
     * Reads V1 format: sentinel already consumed, next VInt is timeSeriesCount.
     */
    private static DeserializedState readFromV1(StreamInput in) throws IOException {
        int timeSeriesCount = in.readVInt();
        List<TimeSeries> series = new ArrayList<>(timeSeriesCount);
        for (int i = 0; i < timeSeriesCount; i++) {
            series.add(readTimeSeriesDelta(in));
        }
        UnaryPipelineStage reduceStage = readReduceStage(in);
        return new DeserializedState(series, reduceStage, AggregationExecStats.EMPTY);
    }

    /**
     * Reads V2 format: identical to V1 for time series + reduce stage, then reads exec-stats (7 longs).
     */
    private static DeserializedState readFromV2(StreamInput in) throws IOException {
        int timeSeriesCount = in.readVInt();
        List<TimeSeries> series = new ArrayList<>(timeSeriesCount);
        for (int i = 0; i < timeSeriesCount; i++) {
            series.add(readTimeSeriesDelta(in));
        }
        UnaryPipelineStage reduceStage = readReduceStage(in);
        AggregationExecStats execStats = new AggregationExecStats(in);
        return new DeserializedState(series, reduceStage, execStats);
    }

    /**
     * Reads the reduce stage from the stream (shared across all versions).
     */
    private static UnaryPipelineStage readReduceStage(StreamInput in) throws IOException {
        boolean hasReduceStage = in.readBoolean();
        if (hasReduceStage) {
            String stageName = in.readString();
            return (UnaryPipelineStage) PipelineStageFactory.readFrom(in, stageName);
        }
        return null;
    }

    // ==================== Version-dispatched serialization ====================

    /**
     * Writes the InternalTimeSeries data to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        switch (serialFormatSetting) {
            case 0:
                doWriteToV0(out);
                break;
            case 1:
                doWriteToV1(out);
                break;
            case 2:
                doWriteToV2(out);
                break;
            default:
                throw new IllegalStateException("Unsupported serial format setting: " + serialFormatSetting);
        }
    }

    /**
     * V0 (legacy) write: no sentinel, per-sample serialization.
     */
    private void doWriteToV0(StreamOutput out) throws IOException {
        out.writeVInt(timeSeries.size());
        for (TimeSeries series : timeSeries) {
            out.writeInt(0); // hash - placeholder for now
            SampleList samples = series.getSamples();
            out.writeVInt(samples.size());
            for (Sample sample : samples) {
                sample.writeTo(out);
            }
            writeSeriesMetadata(series, out);
        }
        writeReduceStage(out);
    }

    /**
     * V1 write: sentinel -1, delta-encoded SampleList.
     */
    private void doWriteToV1(StreamOutput out) throws IOException {
        out.writeVInt(-1);
        out.writeVInt(timeSeries.size());
        for (TimeSeries series : timeSeries) {
            out.writeInt(0); // hash - placeholder for now
            SampleList.writeTo(series.getSamples(), out);
            writeSeriesMetadata(series, out);
        }
        writeReduceStage(out);
    }

    /**
     * V2 write: sentinel -2, delta-encoded SampleList, then 7 exec-stats longs.
     */
    private void doWriteToV2(StreamOutput out) throws IOException {
        out.writeVInt(-2);
        out.writeVInt(timeSeries.size());
        for (TimeSeries series : timeSeries) {
            out.writeInt(0); // hash - placeholder for now
            SampleList.writeTo(series.getSamples(), out);
            writeSeriesMetadata(series, out);
        }
        writeReduceStage(out);
        execStats.writeTo(out);
    }

    /**
     * Writes series metadata (labels, alias, timestamps) shared across V0/V1/V2.
     * Note: V0 writes labels after per-sample loop; V1/V2 write labels after SampleList.writeTo.
     * Both write the same metadata fields in the same order after the samples.
     */
    private static void writeSeriesMetadata(TimeSeries series, StreamOutput out) throws IOException {
        Map<String, String> labelsMap = series.getLabels() != null ? series.getLabels().toMapView() : new HashMap<>();
        out.writeMap(labelsMap, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalString(series.getAlias());
        out.writeLong(series.getMinTimestamp());
        out.writeLong(series.getMaxTimestamp());
        out.writeLong(series.getStep());
    }

    /**
     * Writes the reduce stage information shared across all versions.
     */
    private void writeReduceStage(StreamOutput out) throws IOException {
        if (reduceStage != null) {
            out.writeBoolean(true);
            out.writeString(reduceStage.getName());
            reduceStage.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "time_series"
     */
    @Override
    public String getWriteableName() {
        return "time_series";
    }

    /**
     * Reduces multiple InternalTimeSeries aggregations into a single result.
     *
     * <p>This method handles two scenarios:</p>
     * <ul>
     * <li><strong>With reduce stage:</strong> Delegates to the stage's reduce method</li>
     * <li><strong>Without reduce stage:</strong> Merges time series by labels using {@link SampleMerger}</li>
     * </ul>
     *
     * <p>Circuit breaker tracking is performed to protect coordinator nodes (including
     * data cluster coordinators in CCS setups) from OOM conditions.</p>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     * @throws IllegalArgumentException if any aggregation is not a TimeSeriesProvider
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        try (ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerConsumer.createConsumer(reduceContext)) {
            // Merge exec stats from all input InternalTimeSeries across both code paths
            AggregationExecStats mergedStats = aggregations.stream()
                .filter(a -> a instanceof InternalTimeSeries)
                .map(a -> ((InternalTimeSeries) a).getExecStats())
                .reduce(AggregationExecStats.EMPTY, AggregationExecStats::merge);

            // If we have a reduce stage, delegate directly to it (skip merging)
            if (reduceStage != null) {
                // Track ArrayList allocation for providers list
                cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

                // Convert aggregations to TimeSeriesProvider list for the stage's reduce method
                List<TimeSeriesProvider> timeSeriesProviders = new ArrayList<>(aggregations.size());
                for (InternalAggregation agg : aggregations) {
                    if (!(agg instanceof TimeSeriesProvider)) {
                        throw new IllegalArgumentException("aggregation: " + agg + " is not a TimeSeriesProvider");
                    }
                    timeSeriesProviders.add((TimeSeriesProvider) agg);
                }

                // Use the stage's own reduce method with circuit breaker tracking
                InternalAggregation result = reduceStage.reduce(timeSeriesProviders, reduceContext.isFinalReduce(), cbConsumer);
                // Propagate merged exec stats to the result
                if (result instanceof InternalTimeSeries its) {
                    return new InternalTimeSeries(its.name, its.timeSeries, its.metadata, its.reduceStage, mergedStats);
                }
                return result;
            }

            // No reduce stage - collect all time series from all aggregations and merge by labels
            // Track HashMap base overhead
            cbConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

            Map<Labels, TimeSeries> mergedSeriesByLabels = new HashMap<>();

            for (InternalAggregation aggregation : aggregations) {
                if (!(aggregation instanceof TimeSeriesProvider)) {
                    throw new IllegalArgumentException("aggregation: " + aggregation + " is not a TimeSeriesProvider");
                }
                TimeSeriesProvider provider = (TimeSeriesProvider) aggregation;
                List<TimeSeries> timeSeries = provider.getTimeSeries();

                for (TimeSeries series : timeSeries) {
                    // Use direct Labels comparison for better performance (no string conversion)
                    Labels seriesLabels = series.getLabels();

                    TimeSeries existingSeries = mergedSeriesByLabels.get(seriesLabels);
                    if (existingSeries != null) {
                        // Merge samples from same time series across segments using helper
                        // Use assumeSorted=true for reduce operations as samples should be sorted
                        SampleList mergedSamples = MERGE_HELPER.merge(
                            existingSeries.getSamples(),
                            series.getSamples(),
                            true // assumeSorted - samples should be sorted in reduce phase
                        );

                        // Track merged samples memory
                        cbConsumer.accept(mergedSamples.ramBytesUsed());

                        // Create new merged time series (reuse existing labels and metadata)
                        TimeSeries mergedSeries = new TimeSeries(
                            mergedSamples,
                            existingSeries.getLabels(),
                            existingSeries.getMinTimestamp(),
                            existingSeries.getMaxTimestamp(),
                            existingSeries.getStep(),
                            existingSeries.getAlias()
                        );
                        mergedSeriesByLabels.put(seriesLabels, mergedSeries);
                    } else {
                        // First occurrence of this time series - track HashMap entry + full series (labels + samples)
                        cbConsumer.accept(RamUsageConstants.groupEntryBaseOverhead(seriesLabels) + series.ramBytesUsed());
                        mergedSeriesByLabels.put(seriesLabels, series);
                    }
                }
            }

            // Track result ArrayList allocation
            cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

            List<TimeSeries> combinedTimeSeries = new ArrayList<>(mergedSeriesByLabels.values());

            // Return combined time series (no reduce stage), with merged exec stats
            return new InternalTimeSeries(name, combinedTimeSeries, metadata, null, mergedStats);
        }
    }

    /**
     * Retrieves a property value based on the given path.
     *
     * @param path the property path to retrieve
     * @return the property value
     * @throws IllegalArgumentException if the property path is unknown
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String property = path.get(0);
            if ("timeSeries".equals(property)) {
                return timeSeries;
            }
        }
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for TimeSeriesUnfoldAggregation [" + name + "]");
    }

    /**
     * Returns the list of time series contained in this aggregation result.
     *
     * @return the list of time series data
     */
    public List<TimeSeries> getTimeSeries() {
        return timeSeries;
    }

    /**
     * Gets the reduce stage associated with this aggregation result.
     *
     * @return the reduce stage, or null if no reduce stage is set
     */
    public UnaryPipelineStage getReduceStage() {
        return reduceStage;
    }

    /**
     * Creates a new TimeSeriesProvider with the given time series data.
     *
     * @param timeSeries the new time series data
     * @return a new InternalTimeSeries instance with the provided data
     */
    @Override
    public TimeSeriesProvider createReduced(List<TimeSeries> timeSeries) {
        return new InternalTimeSeries(name, timeSeries, metadata, reduceStage, this.execStats);
    }

    /**
     * Returns the aggregated execution stats carried by this result.
     * On the coordinator, this contains the sum of shard-level stats from all contributing shards.
     *
     * @return the execution stats; never null (falls back to {@link AggregationExecStats#EMPTY})
     */
    public AggregationExecStats getExecStats() {
        return execStats;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("timeSeries");
        for (TimeSeries series : timeSeries) {
            builder.startObject();
            builder.field("hash", 0); // placeholder for now
            if (series.getAlias() != null) {
                builder.field("alias", series.getAlias());
            }
            builder.field("minTimestamp", series.getMinTimestamp());
            builder.field("maxTimestamp", series.getMaxTimestamp());
            builder.field("step", series.getStep());
            builder.startArray("samples");
            for (Sample sample : series.getSamples()) {
                builder.startObject();
                builder.field("timestamp", sample.getTimestamp());
                builder.field("value", sample.getValue());
                builder.endObject();
            }
            builder.endArray();
            // Include labels information in XContent if available
            if (series.getLabels() != null && !series.getLabels().isEmpty()) {
                builder.field("labels", series.getLabels().toMapView());
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    // ==================== TimeSeries reading helpers ====================

    /**
     * Reads a single TimeSeries using delta-encoded SampleList (V1/V2).
     */
    private static TimeSeries readTimeSeriesDelta(StreamInput in) throws IOException {
        int hash = in.readInt();
        SampleList samples = SampleList.readFrom(in);

        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);

        String alias = in.readOptionalString();

        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();

        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    /**
     * Reads a single TimeSeries using per-sample serialization (V0 legacy).
     */
    private static TimeSeries readTimeSeriesLegacy(StreamInput in) throws IOException {
        int hash = in.readInt();
        int sampleCount = in.readVInt();
        List<Sample> samples = new ArrayList<>(sampleCount);

        for (int i = 0; i < sampleCount; i++) {
            samples.add(Sample.readFrom(in));
        }

        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);

        String alias = in.readOptionalString();

        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();

        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    // ==================== equals / hashCode ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTimeSeries that = (InternalTimeSeries) o;
        // execStats is intentionally excluded: it is a side-channel diagnostic, not part of result
        // identity. Two InternalTimeSeries with identical time-series data but different exec stats
        // (e.g. collected from different cluster topologies) represent the same query result and
        // must compare equal.
        return Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && timeSeriesListEquals(timeSeries, that.timeSeries)
            && Objects.equals(
                reduceStage != null ? reduceStage.getName() : null,
                that.reduceStage != null ? that.reduceStage.getName() : null
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            getName(),
            getMetadata(),
            timeSeriesListHashCode(timeSeries),
            reduceStage != null ? reduceStage.getName() : null
        );
    }

    /**
     * Compare two time series lists for equality.
     */
    private boolean timeSeriesListEquals(List<TimeSeries> list1, List<TimeSeries> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;

        for (int i = 0; i < list1.size(); i++) {
            TimeSeries ts1 = list1.get(i);
            TimeSeries ts2 = list2.get(i);

            // Compare key fields
            if (!Objects.equals(ts1.getAlias(), ts2.getAlias())) return false;
            if (ts1.getMinTimestamp() != ts2.getMinTimestamp()) return false;
            if (ts1.getMaxTimestamp() != ts2.getMaxTimestamp()) return false;
            if (ts1.getStep() != ts2.getStep()) return false;
            if (!ts1.getLabels().toMapView().equals(ts2.getLabels().toMapView())) return false;
            if (ts1.getSamples().size() != ts2.getSamples().size()) return false;
        }
        return true;
    }

    /**
     * Compute hash code for time series list.
     */
    private int timeSeriesListHashCode(List<TimeSeries> list) {
        if (list == null) return 0;
        int result = 1;
        for (TimeSeries ts : list) {
            result = 31 * result + (ts == null
                ? 0
                : Objects.hash(ts.getAlias(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getSamples().size()));
        }
        return result;
    }
}
