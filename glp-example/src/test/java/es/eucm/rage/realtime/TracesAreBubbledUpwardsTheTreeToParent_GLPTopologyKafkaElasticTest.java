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
package es.eucm.rage.realtime;

import com.google.gson.Gson;
import es.eucm.rage.realtime.simple.Analysis;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files TRACES_FILES
 */
public class TracesAreBubbledUpwardsTheTreeToParent_GLPTopologyKafkaElasticTest {

	private static final String[] TRACES_FILES = { "DSIR", "KXIH", "SSYP",
			"TQBG", "ZEHU" };
	private static final String NOW_DATE = String.valueOf(
			(new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss").format(new Date())))
			.toLowerCase();
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";
	private static final String BOOTSTRAP_SERVERS = "0.0.0.0:9092";
	private static final String TOPIC = "glp-test-topic-default-kibana-analysis-"
			+ NOW_DATE;
	private static final Producer<String, String> producer = createProducer();

	private static final Gson gson = new Gson();

	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runProducer(final Map trace) {

		try {
			String msg = gson.toJson(trace, Map.class);
			String index = "test";
			final ProducerRecord<String, String> record = new ProducerRecord<>(
					TOPIC, index, msg);

			RecordMetadata metadata = producer.send(record).get();

			System.out.printf("sent record(key=%s value=%s) "
					+ "meta(partition=%d, offset=%d)\n", record.key(),
					record.value(), metadata.partition(), metadata.offset());

		} catch (Exception e) {
			assertTrue("Error sending to Kafka " + e.getMessage() + " cause "
					+ e.getCause(), false);
		} finally {
			producer.flush();
			// producer.close();
		}
	}

	@Test
	public void test() throws Exception {

		/**
		 * Simple GLP: PARENT: "parent-" + NOW_DATE -> GLP_ID
		 * 
		 * FIRST CHILD: "1-" + NOW_DATE -> leaf -> receives traces SECOND CHILD:
		 * "2-" + NOW_DATE -> leaf -> receives traces
		 * **/
		String firstIndex = "1-" + NOW_DATE;
		String secondIndex = "2-" + NOW_DATE;
		String parentIndex = "parent-" + NOW_DATE; // GLP_ID
		String analyticsGLPId = ESUtils.getAnalyticsGLPIndex(parentIndex);

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();
		RestHighLevelClient hClient = new RestHighLevelClient(client);

		Map parentAnalytics = new HashMap();
		parentAnalytics.put("testkey", "testval");
		IndexRequest indexParent = new IndexRequest(analyticsGLPId,
				"analytics", parentIndex).source(parentAnalytics);

		Map firstChildAnalytics = new HashMap();
		firstChildAnalytics.put(TopologyBuilder.ANALYTICS_PARENT_ID_KEY,
				parentIndex);
		IndexRequest indexFirstChild = new IndexRequest(analyticsGLPId,
				"analytics", firstIndex).source(firstChildAnalytics);

		Map secondChildAnalytics = new HashMap();
		secondChildAnalytics.put(TopologyBuilder.ANALYTICS_PARENT_ID_KEY,
				parentIndex);
		IndexRequest indexSecondChild = new IndexRequest(analyticsGLPId,
				"analytics", secondIndex).source(secondChildAnalytics);

		hClient.index(indexParent);
		hClient.index(indexFirstChild);
		hClient.index(indexSecondChild);

		CSVToMapTrace parser = new CSVToMapTrace(analyticsGLPId);
		Map<String, Integer> res = new HashMap<>();
		List<Map> firstChildTraces = new ArrayList<>();
		List<Map> secondChildTraces = new ArrayList<>();
		List<Map> parentChildTraces = new ArrayList<>();

		for (int i = 0; i < TRACES_FILES.length; ++i) {
			String idx;
			if (i < 3) {
				idx = firstIndex;
			} else {
				idx = secondIndex;
			}
			List<List<Object>> tuples = parser.getTuples("glp/"
					+ TRACES_FILES[i] + ".csv", idx, i);
			for (List tupleNestedList : tuples) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;
					runProducer(traceMap);

					if (i < 3) {
						firstChildTraces.add(traceMap);
					} else {
						secondChildTraces.add(traceMap);
					}
					parentChildTraces.add(traceMap);
				}
			}

			Integer current = res.get(idx);
			if (current == null) {
				res.put(idx, parser.getCompletedSuccessfully());
			} else {
				res.put(idx, current + parser.getCompletedSuccessfully());
			}

			Integer currentParent = res.get(parentIndex);
			if (currentParent == null) {
				res.put(parentIndex, parser.getCompletedSuccessfully());
			} else {
				res.put(parentIndex,
						currentParent + parser.getCompletedSuccessfully());
			}
		}

		new Thread(new Runnable() {
			@Override
			public void run() {

				try {
					runConsumer(firstChildTraces, secondChildTraces,
							parentChildTraces, firstIndex, secondIndex,
							parentIndex);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();

		Map<String, Object> conf = new HashedMap();
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, TOPIC);

		StormTopology topology = new Analysis().getTopology(conf);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime-" + NOW_DATE, conf, topology);

		try {
			Thread.sleep(35000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		assertTrue("All parent traces should have been sent to kafka",
				parentChildTraces.isEmpty());

		for (Map.Entry<String, Integer> entry : res.entrySet()) {

			Response response = client.performRequest("GET",
					"/" + entry.getKey() + "/_search?size=5000&q=*:*");
			int status = response.getStatusLine().getStatusCode();

			assertEquals("TEST GET error, status is" + status, status,
					HttpStatus.SC_OK);

			String responseString = EntityUtils.toString(response.getEntity());
			Map<String, Object> responseDocs = (Map) gson.fromJson(
					responseString, Map.class);

			Map hits = (Map) responseDocs.get("hits");

			int total = ((Double) hits.get("total")).intValue();

			assertEquals("Total traces " + entry.getValue() + ", current "
					+ total, entry.getValue().intValue(), total);
		}
	}

	private static Consumer<Long, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer"
				+ NOW_DATE);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	static void runConsumer(List<Map> firstChildTraces, List<Map> secondChildTraces, List<Map> parentTraces,
                            String firstIndex, String secondIndex, String parentIndex) throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                Map trace = gson.fromJson(record.value(), Map.class);
                String activityId = trace.get(TopologyBuilder.ACTIVITY_ID_KEY).toString();
                if (activityId.equalsIgnoreCase(firstIndex)) {
                    for (int i = 0; i < firstChildTraces.size(); ++i) {
                        Map originalTrace = firstChildTraces.get(i);
                        if (originalTrace.get(TopologyBuilder.UUIDV4).toString().equalsIgnoreCase(trace.get(TopologyBuilder.UUIDV4).toString())) {
                            firstChildTraces.remove(i);
                            break;
                        }
                    }
                } else if (activityId.equalsIgnoreCase(secondIndex)) {
                    for (int i = 0; i < secondChildTraces.size(); ++i) {
                        Map originalTrace = secondChildTraces.get(i);
                        if (originalTrace.get(TopologyBuilder.UUIDV4).toString().equalsIgnoreCase(trace.get(TopologyBuilder.UUIDV4).toString())) {
                            secondChildTraces.remove(i);
                            break;
                        }
                    }
                } else if (activityId.equalsIgnoreCase(parentIndex)) {
                    for (int i = 0; i < parentTraces.size(); ++i) {
                        Map originalTrace = parentTraces.get(i);
                        if (originalTrace.get(TopologyBuilder.UUIDV4).toString().equalsIgnoreCase(trace.get(TopologyBuilder.UUIDV4).toString())) {
                            parentTraces.remove(i);
                            break;
                        }
                    }
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
