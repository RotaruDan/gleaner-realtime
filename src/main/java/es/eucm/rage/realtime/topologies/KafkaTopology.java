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

	public KafkaTopology(String sessionId) {
		this.sessionId = sessionId;
	}

	public void prepare(String zookeeperUrl, ESStateFactory elasticStateFactory) {
		BrokerHosts zk = new ZkHosts(zookeeperUrl);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, sessionId,
				sessionId);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		super.prepare(newStream("kafka-spout", spout), elasticStateFactory);
	}

	@Override
	protected Stream createTracesStream(Stream stream) {
		return stream.each(new Fields("str"), new JsonToTrace(sessionId),
				new Fields("versionId", "trace"));
	}
}
