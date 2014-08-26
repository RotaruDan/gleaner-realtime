package es.eucm.gleaner.realtime.topologies;

import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import es.eucm.gleaner.realtime.filters.FieldValueFilter;
import es.eucm.gleaner.realtime.functions.TraceFieldExtractor;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;

public class RealtimeTopology extends TridentTopology {

	private Stream zoneStream;

	public RealtimeTopology(ITridentSpout spout) {
		init(newStream("traces", spout));
	}

	public RealtimeTopology(IRichSpout spout) {
		init(newStream("traces", spout));
	}

	public void init(Stream tracesStream) {
		Stream eventStream = tracesStream.each(new Fields("trace"),
				new TraceFieldExtractor("gameplayId", "event"), new Fields(
						"gameplayId", "event"));

		// Zone
		this.zoneStream = eventStream.each(new Fields("event", "trace"),
				new FieldValueFilter("event", "zone")).each(
				new Fields("trace"), new TraceFieldExtractor("value"),
				new Fields("zone"));
	}

	public Stream getZoneStream() {
		return zoneStream;
	}
}
