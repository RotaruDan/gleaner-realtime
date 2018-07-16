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

import java.io.Serializable;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutBuilder {

	public static final String SCHEME_KEY = "str";
	
	private static Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new JustValueFunc();
	
  /**
   * Needs to be serializable
   */
  private static class JustValueFunc implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {
    /**
		 * @see java.io.Serializable 
		 */
		private static final long serialVersionUID = -1055394368828229747L;

			@Override
      public List<Object> apply(ConsumerRecord<String, String> record) {
          return new Values(record.value());
      }
  }

  
	private String topic;
	
	private String zookeeperUrl;

	/**
	 * Builds an {@link OpaqueTridentKafkaSpout} when invoking the method {@link KafkaSpoutBuilder#build()}.
	 */
	public KafkaSpoutBuilder() {}

	public KafkaSpoutBuilder zookeeper(String zookeeperUrl) {
		this.zookeeperUrl = zookeeperUrl;
		return this;
	}

	public KafkaSpoutBuilder topic(String topic) {
		this.topic = topic;
		return this;
	}

	public KafkaTridentSpoutOpaque<String, String> build() {

		return new KafkaTridentSpoutOpaque<>(newKafkaSpoutConfig());

	}

	protected KafkaSpoutConfig<String, String> newKafkaSpoutConfig() {
		return KafkaSpoutConfig.builder(this.zookeeperUrl, this.topic)
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, this.topic + "_" + System.nanoTime())
				.setRecordTranslator(JUST_VALUE_FUNC, new Fields(SCHEME_KEY))
				.build();
	}
}
