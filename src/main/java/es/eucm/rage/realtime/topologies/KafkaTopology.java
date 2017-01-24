/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.topologies;

import es.eucm.rage.realtime.functions.JsonToTrace;
import es.eucm.rage.realtime.states.ESStateFactory;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;

public class KafkaTopology extends RealtimeTopology {

	private String sessionId;

	/**
	 * A {@link RealtimeTopology} that pulls data from Kafka and passes the
	 * traces to the topology as Map<String, Object> objects under the touple
	 * tag "trace".
	 * 
	 * Exports a tuple identified under the field keys <"versionId", "trace">
	 * with the values <sessionId, Map<String, Object>>
	 * 
	 * @param sessionId
	 */
	public KafkaTopology(String sessionId) {
		this.sessionId = sessionId;
	}

	/**
	 * Connects to Kafka topic (identified by "sessionId") using an
	 * {@link OpaqueTridentKafkaSpout}. Note that this spout requires an
	 * additional implementation of opaque values persistence to work correctly.
	 * 
	 * @param zookeeperUrl
	 * @param elasticStateFactory
	 */
	public void prepare(String zookeeperUrl, ESStateFactory elasticStateFactory) {
		// ZooKeeper configuration for the TridentKafkaConfig
		BrokerHosts zk = new ZkHosts(zookeeperUrl);

		// Kafka configuration for the OpaqueTridentKafkaSpout
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, sessionId,
				sessionId);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		// Spout pulling data from Kafka topic "sessionId" under the "str" field
		// key
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		// Prepares a Kafka stream for the {@link RealtimeTopology} analysis
		super.prepare(newStream("kafka-spout", spout), elasticStateFactory);
	}

	@Override
	protected Stream createTracesStream(Stream stream) {
		// Parse string data (identified by "str") to a tuple as follows:
		// Signature <"versionId", "trace">
		// Value <"sessionId", Map<String, Object>> with the value of the trace
		// received from the Kafka topic.
		return stream.each(new Fields("str"), new JsonToTrace(sessionId),
				new Fields("versionId", "trace"));
	}
}
