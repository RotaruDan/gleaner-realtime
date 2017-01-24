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
package es.eucm.rage.realtime;

import es.eucm.rage.realtime.states.ESStateFactory;
import es.eucm.rage.realtime.topologies.KafkaTopology;
import es.eucm.rage.realtime.utils.DBUtils;
import es.eucm.rage.realtime.utils.EsConfig;
import org.apache.storm.generated.StormTopology;

import java.util.Map;

/**
 * Startup class for the analysis, see:
 * https://github.com/apache/storm/tree/master/external/flux#existing-topologies
 */
public class Analysis {

	/**
	 * Builds a KafkaTopology
	 * 
	 * @param conf
	 *            Map object with the 'flux.yml' contents (contains sessionId,
	 *            zookeeperUrl, elasticsearchUrl)
	 * @param sessionId
	 *            Used for the creation of ElasticSearch indices:
	 *            {@link DBUtils#getTracesIndex(String)} and
	 *            {@link DBUtils#getResultsIndex(String)}
	 * @param zookeeperUrl
	 *            Used to connect to Kafka and pull data from the topic
	 *            'sessionId'
	 * @return a topology that connects to kafka and performs the realtime
	 *         analysis
	 */
	private static StormTopology buildTopology(Map conf, String sessionId,
			String zookeeperUrl) {
		DBUtils.startRealtime(sessionId);
		KafkaTopology kafkaTopology = new KafkaTopology(sessionId);
		String elasticSearchUrl = conf.get("elasticsearchUrl").toString();
		EsConfig esConfig = new EsConfig(elasticSearchUrl, sessionId);
		kafkaTopology.prepare(zookeeperUrl, new ESStateFactory(esConfig));
		return kafkaTopology.build();
	}

	// Storm Flux Start-up function
	// (https://github.com/apache/storm/tree/master/external/flux#existing-topologies)
	public StormTopology getTopology(Map<String, Object> conf) {
		/*
		 * Note that 'conf' object contains the Storm Flux configuration
		 * parameters (defined in the 'flux.yml' file). As a Java Map, the
		 * values can easily be accessed. For instance:
		 * 
		 * String sessionId = conf.get("sessionId").toString(); String
		 * zookeeperUrl = conf.get("zookeeperUrl").toString(); String
		 * elasticsearchUrl= conf.get("elasticsearchUrl").toString();
		 * 
		 * For more information check out: Storm Flux, 'Existing Topologies'
		 * documentation:
		 * https://github.com/apache/storm/tree/master/external/flux
		 * #existing-topologies RAGE Analytics Backend 'flux.yml' format:
		 * https:/
		 * /github.com/e-ucm/rage-analytics-backend/blob/master/default-flux.yml
		 */
		String sessionId = conf.get("sessionId").toString();
		String zookeeperUrl = conf.get("zookeeperUrl").toString();
		return buildTopology(conf, sessionId, zookeeperUrl);
	}
}
