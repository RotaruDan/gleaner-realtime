package es.eucm.gleaner.realtime.topologies;

import backtype.storm.tuple.Fields;
import es.eucm.gleaner.realtime.filters.FieldValueFilter;
import es.eucm.gleaner.realtime.functions.PropertyCreator;
import es.eucm.gleaner.realtime.functions.TraceFieldExtractor;
import es.eucm.gleaner.realtime.states.GameplayStateUpdater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

public class RealtimeTopology extends TridentTopology {

    public <T> void prepare(ITridentSpout<T> spout, StateFactory stateFactory) {
        prepare(newStream("traces", spout), stateFactory);
    }

    /**
     * @param traces a stream with two fields: a versionId {@link String} and a
     *               trace {@link java.util.Map}
     */
    public void prepare(Stream traces, StateFactory stateFactory) {

        StateUpdater stateUpdater = new GameplayStateUpdater();

        Stream eventStream = createTracesStream(traces).each(
                new Fields("trace"),
                new TraceFieldExtractor("gameplayId", "event"),
                new Fields("gameplayId", "event"));


        // Zones
        eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "zone"))
                .each(new Fields("trace"), new TraceFieldExtractor("value"),
                        new Fields("value"))
                .each(new Fields("event", "value"),
                        new PropertyCreator("value", "event"),
                        new Fields("p", "v"))
                .partitionPersist(stateFactory,
                        new Fields("versionId", "gameplayId", "p", "v"),
                        stateUpdater);


        // Variables
        eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "var"))
                .each(new Fields("trace"),
                        new TraceFieldExtractor("target", "value"),
                        new Fields("var", "value"))
                .each(new Fields("event", "var", "value"),
                        new PropertyCreator("value", "event", "var"),
                        new Fields("p", "v"))
                .partitionPersist(stateFactory,
                        new Fields("versionId", "gameplayId", "p", "v"),
                        stateUpdater);


        // Choices
        eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "choice"))
                .each(new Fields("trace"),
                        new TraceFieldExtractor("target", "value"),
                        new Fields("choice", "option"))
                .groupBy(
                        new Fields("versionId", "gameplayId", "event",
                                "choice", "option"))
                .persistentAggregate(stateFactory, new Count(),
                        new Fields("count"));

        // Interactions
        eventStream
                .each(new Fields("event", "trace"),
                        new FieldValueFilter("event", "interact"))
                .each(new Fields("trace"), new TraceFieldExtractor("target"),
                        new Fields("target"))
                .groupBy(
                        new Fields("versionId", "gameplayId", "event", "target"))
                .persistentAggregate(stateFactory, new Count(),
                        new Fields("count"));

    }

    protected Stream createTracesStream(Stream stream) {
        return stream;
    }
}
