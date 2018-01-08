/**
 * Copyright © 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.topologies;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class KafkaSpoutBuilder {

	private String topic;
	private String zookeeperUrl;

	/**
	 * Builds an {@link OpaqueTridentKafkaSpout} when invoking the method
	 * {@link KafkaSpoutBuilder#build()}.
	 */
	public KafkaSpoutBuilder() {
	}

	public KafkaSpoutBuilder zookeeper(String zookeeperUrl) {
		this.zookeeperUrl = zookeeperUrl;
		return this;
	}

	public KafkaSpoutBuilder topic(String topic) {
		this.topic = topic;
		return this;
	}

	public OpaqueTridentKafkaSpout build() {

		// ZooKeeper configuration for the TridentKafkaConfig
		BrokerHosts zk = new ZkHosts(this.zookeeperUrl);

		// Kafka configuration for the OpaqueTridentKafkaSpout
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic, topic);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		// Spout pulling data from Kafka topic "sessionId" under the "str" field
		// key
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		return spout;
	}

}
