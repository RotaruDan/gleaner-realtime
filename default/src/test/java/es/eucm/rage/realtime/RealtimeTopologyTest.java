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

import es.eucm.rage.realtime.simple.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import es.eucm.rage.realtime.states.elasticsearch.EsMapState;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files TRACES_FILES
 */
public class RealtimeTopologyTest {

	private static final String[] TRACES_FILES = { "DSIR", "KXIH", "SSYP",
			"TQBG", "ZEHU" };
	private static final String NOW_DATE = new Date().toString().toLowerCase()
			.trim().replace(" ", "-");
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";

	@Test
	public void test() throws IOException {

		FeederBatchSpout tracesSpout = new FeederBatchSpout(Arrays.asList(
				TopologyBuilder.SESSION_ID_KEY,
				es.eucm.rage.realtime.topologies.TopologyBuilder.TRACE_KEY));

		TridentTopology topology = new TridentTopology();

		EsState.Factory partitionPersist = new EsState.Factory();
		StateFactory persistentAggregateFactory = new EsMapState.Factory();

		// Test topology Builder configuration
		new TopologyBuilder().build(topology,
				topology.newStream("testFileStream", tracesSpout),
				partitionPersist, persistentAggregateFactory);

		Config conf = new Config();
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.SESSION_ID_FLUX_PARAM, NOW_DATE);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology.build());

		CSVToMapTrace parser = new CSVToMapTrace(NOW_DATE);
		int totalTraces = 0;
		for (int i = 0; i < TRACES_FILES.length; ++i) {
			List tuples = parser
					.getTuples("traces/" + TRACES_FILES[i] + ".csv");
			tracesSpout.feed(tuples);
			totalTraces += tuples.size();
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		TransportClient client = partitionPersist
				.makeElasticsearchClient(Arrays.asList(InetAddress
						.getByName(ES_HOST)));

		SearchResponse tracesSearch = client
				.prepareSearch(ESUtils.getTracesIndex(NOW_DATE))
				.setQuery(QueryBuilders.matchAllQuery()).setSize(10000).get();

		SearchHit[] hits = tracesSearch.getHits().getHits();
		/*
		 * for (SearchHit hit : hits) { //Handle the hit... }
		 */
		assertEquals(
				"Total traces " + totalTraces + ", current " + hits.length,
				totalTraces, hits.length);

		String resultsIndex = ESUtils.getResultsIndex(NOW_DATE);
		for (int i = 0; i < TRACES_FILES.length; ++i) {
			List<String> lines = parser.getLines("traces/results/"
					+ TRACES_FILES[i] + ".csv-result");

			Map playerState = client
					.prepareGet(resultsIndex, ESUtils.getResultsType(),
							"traces/" + TRACES_FILES[i] + ".csv")
					.setOperationThreaded(false).get().getSourceAsMap();

			for (String line : lines) {
				String[] keyValue = line.split("=");
				String flatObjectKey = keyValue[0];
				String[] keys = flatObjectKey.split("\\.");

				Map propertyMap = playerState;
				for (int j = 0; j < keys.length - 1; ++j) {
					propertyMap = (Map) propertyMap.get(keys[j]);
				}
				Object value = propertyMap.get(keys[keys.length - 1]);

				if (flatObjectKey
						.startsWith(es.eucm.rage.realtime.topologies.TopologyBuilder.TraceEventTypes.PROGRESSED)
						|| flatObjectKey
								.startsWith(es.eucm.rage.realtime.topologies.TopologyBuilder.TraceEventTypes.COMPLETED)) {

					assertEquals(flatObjectKey, value, keyValue[1]);
				} else {
					assertEquals(flatObjectKey, value,
							Integer.valueOf(keyValue[1]));
				}
			}
		}

	}

}
