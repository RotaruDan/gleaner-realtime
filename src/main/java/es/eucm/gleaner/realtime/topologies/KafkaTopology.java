package es.eucm.gleaner.realtime.topologies;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import es.eucm.gleaner.realtime.functions.JsonToTrace;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.state.StateFactory;

public class KafkaTopology extends RealtimeTopology {

	private String sessionId;

	public KafkaTopology(String sessionId) {
		this.sessionId = sessionId;
	}

	public void prepare(StateFactory stateFactory, String zookeeperUrl) {
		BrokerHosts zk = new ZkHosts(zookeeperUrl);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, sessionId,
				sessionId);
		spoutConf.forceFromStart = true;
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		super.prepare(newStream("kafka-spout", spout), stateFactory);
	}

	@Override
	protected Stream createTracesStream(Stream stream) {
		return stream.each(new Fields("str"), new JsonToTrace(sessionId),
				new Fields("versionId", "trace"));
	}
}

