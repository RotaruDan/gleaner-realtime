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
package es.eucm.rage.realtime.states.elasticsearch;

import es.eucm.rage.realtime.AbstractAnalysis;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.Document;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class EsState implements State {

	private static final Logger LOG = LoggerFactory.getLogger(EsState.class);

	private final RestHighLevelClient hClient;

	@Override
	public void beginCommit(Long aLong) {

	}

	@Override
	public void commit(Long aLong) {

	}

	private RestClient _client;

	public EsState(RestClient client, RestHighLevelClient hClient) {
		_client = client;
		this.hClient = hClient;
	}

	public void bulkUpdateIndices(List<TridentTuple> inputs) {

		try {
			BulkRequest request = new BulkRequest();

			for (TridentTuple input : inputs) {
				Document<Map> doc = (Document<Map>) input.get(0);
				Map source = doc.getSource();

				String index = ESUtils.getTracesIndex(doc.getIndex());
				String indexPrefix = doc.getIndexPrefix();
				if (indexPrefix != null) {
					index = indexPrefix + "-" + index;
				}

				String type = doc.getType();
				if (type == null) {
					type = ESUtils.getTracesType();
				}

				DocWriteRequest req;
				Object uuidv4 = source.get(TopologyBuilder.UUIDV4);
				if (uuidv4 != null) {
					req = new UpdateRequest(index, type, uuidv4.toString())
							.docAsUpsert(true).doc(source).retryOnConflict(20);
				} else {
					req = new IndexRequest(index, type).source(source);
				}

				request.add(req);
			}

			BulkResponse bulkResponse = hClient.bulk(request);

			if (bulkResponse.hasFailures()) {
				LOG.error("BULK hasFailures proceeding to re-bulk");
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (bulkItemResponse.isFailed()) {
						BulkItemResponse.Failure failure = bulkItemResponse
								.getFailure();
						LOG.error("Failure " + failure.getCause()
								+ ", response: " + bulkItemResponse);

						BulkResponse bulkResponse2 = hClient.bulk(request);
						if (bulkResponse2.hasFailures()) {
							LOG.error("BULK hasFailures proceeding to re-bulk");
							for (BulkItemResponse bulkItemResponse2 : bulkResponse2) {
								if (bulkItemResponse2.isFailed()) {
									BulkItemResponse.Failure failure2 = bulkItemResponse2
											.getFailure();
									LOG.error("Failure " + failure2.getCause()
											+ ", response: "
											+ bulkItemResponse2);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			LOG.error("error while executing bulk request to elasticsearch, "
					+ "failed to store data into elasticsearch", e);
		}
	}

	public void setProperty(String activityId, String name, String key,
			Object value) {

		try {

			Map<String, Object> map = new HashMap<>();
			String[] keys = key.split("\\.");
			Map nested = map;
			for (int i = 0; i < keys.length - 1; ++i) {
				Map<String, Object> keymap = new HashMap<>();
				nested.put(keys[i], keymap);
				nested = keymap;
			}

			nested.put(keys[keys.length - 1], value);

			hClient.update(new UpdateRequest(ESUtils
					.getResultsIndex(activityId), ESUtils.getResultsType(),
					name).docAsUpsert(true).doc(map).retryOnConflict(50));

		} catch (Exception e) {
			LOG.error("Set Property has failures : {}", e);
		}
	}

	public void updateUniqueArray(String glpId, String activityId, String key,
			String name, String childId) {
		try {

			UpdateRequest update = new UpdateRequest(glpId, "analytics",
					activityId);
			Map params = new HashMap(1);
			params.put("name", name);

			String script = "if (ctx._source.containsKey(\"" + key + "\")) {"
					+ "if (!ctx._source." + key + ".contains(params.name)) {"
					+ "ctx._source." + key + ".add(params.name);" + "}"
					+ "} else {" + "ctx._source." + key + " = [params.name];"
					+ "}";

			if (childId != null) {
				List full = new ArrayList(1);
				full.add(childId);
				Map fullCompleted = new HashMap();
				fullCompleted.put(name, full);
				params.put("fullCompleted", fullCompleted);
				script += "if (ctx._source.containsKey(\"fullCompleted\")) {"
						+ "if (ctx._source.fullCompleted.containsKey(params.name)) { "
						+ "if(!ctx._source.fullCompleted[params.name].contains(params.fullCompleted[params.name][0])) { "
						+ "ctx._source.fullCompleted[params.name].add(params.fullCompleted[params.name][0]);"
						+ "}"
						+ "} else {"
						+ "ctx._source.fullCompleted[params.name] = params.fullCompleted[params.name];"
						+ "}"
						+ "} else {"
						+ "ctx._source[\"fullCompleted\"] = params.fullCompleted;"
						+ "}";
			}
			update.script(new Script(ScriptType.INLINE, "painless", script,
					params));
			update.retryOnConflict(50);
			hClient.update(update);

		} catch (Exception e) {
			LOG.error("updateUniqueArray has failures : {}", e);
		}
	}

	public void updatePerformanceArray(String performanceIndex, String classId,
			String timestamp, String name, float score) {
		try {

			GetRequest getRequest2 = new GetRequest(performanceIndex,
					ESUtils.getResultsType(), classId);
			getRequest2.fetchSourceContext(new FetchSourceContext(false));
			getRequest2.storedFields("_none_");

			boolean exists2 = hClient.exists(getRequest2);

			if (!exists2) {
				IndexRequest indexRequest = new IndexRequest(performanceIndex,
						ESUtils.getResultsType(), classId);
				indexRequest.source(new HashMap());
				try {
					hClient.index(indexRequest);
				} catch (Exception exp) {
					LOG.info("Not created index");
					exp.printStackTrace();
				}
			}

			UpdateRequest update = new UpdateRequest(performanceIndex,
					ESUtils.getResultsType(), classId);

			Date date = new SimpleDateFormat("yyyy-MM-dd").parse(timestamp);

			Calendar cal = Calendar.getInstance();
			cal.setTime(date);

			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH);
			int week = cal.get(Calendar.WEEK_OF_YEAR);

			Map params = new HashMap(3);
			params.put("name", name);
			params.put("score", score);
			params.put("year", year + "");
			params.put("month", month + "");
			params.put("week", week + "");
			Map obj = new HashMap();
			List list = new ArrayList();
			Map student = new HashMap();
			student.put("student", name);
			student.put("score", score);
			student.put("n", 1);
			list.add(student);
			obj.put("students", list);
			params.put("obj", obj);

			Map months = new HashMap();
			Map tmpStudents = new HashMap();
			tmpStudents.put("students", list);
			months.put(month + "", tmpStudents);
			params.put("months", months);

			Map weeks = new HashMap();
			weeks.put(week + "", tmpStudents);
			params.put("weeks", weeks);

			String script = ""
					+ "if (!ctx._source.containsKey(params.year)) {"
					+ "	ctx._source[params.year] = params.obj;"
					+ "} else if (!ctx._source[params.year].containsKey(\"students\")) {"
					+ "	ctx._source[params.year][\"students\"] = params.obj.students;"
					+ "} else {"
					+ "   def found = false;"
					+ "   for(int i = 0; i < ctx._source[params.year].students.length; ++i){"
					+ "       def val = ctx._source[params.year].students[i];"
					+ "       if(!found && val.student == params.name){"
					+ "           found = true;"
					+ "           def n = val.n + 1;"
					+ "           val.score = ((val.score * val.n) / (n)) + (params.score / n);"
					+ "           val.n = n;"
					+ "       }"
					+ "   }"
					+ "   if (!found) {"
					+ "       ctx._source[params.year].students.add(params.obj.students[0]);"
					+ "   }"
					+ "}"
					+ ""
					+ "if (!ctx._source[params.year].containsKey(\"months\")) {"
					+ "   ctx._source[params.year][\"months\"] = params.months;"
					+ "} else if (!ctx._source[params.year][\"months\"].containsKey(params.month)) {"
					+ "   ctx._source[params.year][\"months\"][params.month] = params.months[params.month];"
					+ "} else if (!ctx._source[params.year][\"months\"][params.month].containsKey(\"students\")) {"
					+ "   ctx._source[params.year][\"months\"][params.month][\"students\"] = params.months[params.month].students;"
					+ "} else {"
					+ "   def found = false;"
					+ "   for(int i = 0; i < ctx._source[params.year].months[params.month].students.length; ++i){"
					+ "       def val = ctx._source[params.year].months[params.month].students[i];"
					+ "       if(!found && val.student == params.name){"
					+ "           found = true;"
					+ "           def n = val.n + 1;"
					+ "           val.score = ((val.score * val.n) / (n)) + (params.score / n);"
					+ "           val.n = n;"
					+ "       }"
					+ "   }"
					+ "   if (!found) {"
					+ "       ctx._source[params.year].months[params.month].students.add(params.months[params.month].students[0]);"
					+ "   }"
					+ "}"
					+ ""
					+ "if (!ctx._source[params.year].containsKey(\"weeks\")) {"
					+ "   ctx._source[params.year][\"weeks\"] = params.weeks;"
					+ "} else if (!ctx._source[params.year][\"weeks\"].containsKey(params.week)) {"
					+ "   ctx._source[params.year][\"weeks\"][params.week] = params.weeks[params.week];"
					+ "} else if (!ctx._source[params.year][\"weeks\"][params.week].containsKey(\"students\")) {"
					+ "   ctx._source[params.year][\"weeks\"][params.week][\"students\"] = params.weeks[params.week].students;"
					+ "} else {"
					+ "   def found = false;"
					+ "   for(int i = 0; i < ctx._source[params.year].weeks[params.week].students.length; ++i){"
					+ "       def val = ctx._source[params.year].weeks[params.week].students[i];"
					+ "       if(!found && val.student == params.name){"
					+ "           found = true;"
					+ "           def n = val.n + 1;"
					+ "           val.score = ((val.score * val.n) / (n)) + (params.score / n);"
					+ "           val.n = n;"
					+ "       }"
					+ "   }"
					+ "   if (!found) {"
					+ "       ctx._source[params.year].weeks[params.week].students.add(params.weeks[params.week].students[0]);"
					+ "   }" + "}";
			update.script(new Script(ScriptType.INLINE, "painless", script,
					params));
			update.scriptedUpsert();
			update.retryOnConflict(50);
			hClient.update(update);

		} catch (Exception e) {
			LOG.error("updatePerformanceArray has failures : {}", e);
		}
	}

	public void updateAverageScore(String overallIndex, String name, float score) {
		try {
			GetRequest getRequest2 = new GetRequest(overallIndex,
					ESUtils.getResultsType(), name);
			getRequest2.fetchSourceContext(new FetchSourceContext(false));
			getRequest2.storedFields("_none_");

			boolean exists2 = hClient.exists(getRequest2);

			if (!exists2) {
				IndexRequest indexRequest = new IndexRequest(overallIndex,
						ESUtils.getResultsType(), name);
				indexRequest.source(new HashMap());
				try {
					hClient.index(indexRequest);
				} catch (Exception exp) {
					LOG.info("Not created index");
					exp.printStackTrace();
				}
			}

			UpdateRequest update = new UpdateRequest(overallIndex,
					ESUtils.getResultsType(), name);

			Map params = new HashMap(3);
			params.put("score", score);

			String script = ""
					+ "if (!ctx._source.containsKey(\"min\")) {"
					+ "	ctx._source[\"min\"] = params.score;"
					+ "} else if (ctx._source.min > params.score) {"
					+ "	ctx._source.min = params.score;"
					+ "} "
					+ "if (!ctx._source.containsKey(\"max\")) {"
					+ "	ctx._source[\"max\"] = params.score;"
					+ "} else if (ctx._source.max < params.score) {"
					+ "	ctx._source.max = params.score;"
					+ "}"
					+ "if (!ctx._source.containsKey(\"n\")) {"
					+ "	ctx._source[\"n\"] = 1;"
					+ "} else {"
					+ "	ctx._source.n = ctx._source.n + 1;"
					+ "}"
					+ "if (!ctx._source.containsKey(\"avg\")) {"
					+ "	ctx._source[\"avg\"] = params.score;"
					+ "} else {"
					+ "	ctx._source.avg = ((ctx._source.avg * (ctx._source.n - 1))/ctx._source.n) + (params.score / ctx._source.n);"
					+ "}";
			update.script(new Script(ScriptType.INLINE, "painless", script,
					params));
			update.scriptedUpsert();
			update.retryOnConflict(50);
			hClient.update(update);

		} catch (Exception e) {
			LOG.error("updatePerformanceArray has failures : {}", e);
		}
	}

	public void updateAnswersArray(String overallIndex, String name, Map trace,
			Map analytics, Map rootAnalytics) {
		try {
			GetRequest getRequest2 = new GetRequest(overallIndex,
					ESUtils.getResultsType(), name);
			getRequest2.fetchSourceContext(new FetchSourceContext(false));
			getRequest2.storedFields("_none_");

			boolean exists2 = hClient.exists(getRequest2);

			if (!exists2) {
				IndexRequest indexRequest = new IndexRequest(overallIndex,
						ESUtils.getResultsType(), name);
				indexRequest.source(new HashMap());
				try {
					hClient.index(indexRequest);
				} catch (Exception exp) {
					LOG.info("Not created index");
					exp.printStackTrace();
				}
			}

			UpdateRequest update = new UpdateRequest(overallIndex,
					ESUtils.getResultsType(), name);

			Map out = (Map) trace.get(TopologyBuilder.OUT_KEY);

			String timestamp = out.get(
					TopologyBuilder.TridentTraceKeys.TIMESTAMP).toString();

			Object analyticsNameObject = analytics
					.get((TopologyBuilder.TridentTraceKeys.NAME));
			String analyticsName;
			if (analyticsNameObject != null) {
				analyticsName = analyticsNameObject.toString();
			} else {
				analyticsName = trace.get(TopologyBuilder.GAMEPLAY_ID)
						.toString();
			}

			Object rootAnalyticsNameObject = rootAnalytics
					.get((TopologyBuilder.TridentTraceKeys.NAME));
			String rootAnalyticsName;
			if (rootAnalyticsNameObject != null) {
				rootAnalyticsName = rootAnalyticsNameObject.toString();
			} else {
				rootAnalyticsName = ESUtils.getRootGLPId(trace.get(
						TopologyBuilder.GLP_ID_KEY).toString());
			}

			String target = out.get(TopologyBuilder.TridentTraceKeys.TARGET)
					.toString();
			String response = out
					.get(TopologyBuilder.TridentTraceKeys.RESPONSE).toString();

			boolean success = false;

			int correct, incorrect;
			Object successObject = out
					.get(TopologyBuilder.TridentTraceKeys.SUCCESS);
			if (successObject instanceof Boolean) {
				success = (Boolean) successObject;
			}
			if (success) {
				correct = 1;
				incorrect = 0;
			} else {
				correct = 0;
				incorrect = 1;
			}

			Map answer = new HashMap();
			answer.put("timestamp", timestamp);
			answer.put("rootAnalyticsName", rootAnalyticsName);
			answer.put("analyticsName", analyticsName);
			answer.put("target", target);
			answer.put("response", response);
			answer.put("success", success);

			Map params = new HashMap(3);
			params.put("answer", answer);
			params.put("correct", correct);
			params.put("incorrect", incorrect);

			String script = ""
					+ "if (!ctx._source.containsKey(\"correct\")) {"
					+ "	ctx._source[\"correct\"] = params.correct;"
					+ "} else {"
					+ "   ctx._source[\"correct\"] = ctx._source[\"correct\"] + params.correct;"
					+ "}"
					+ "if (!ctx._source.containsKey(\"incorrect\")) {"
					+ "	ctx._source[\"incorrect\"] = params.incorrect;"
					+ "} else {"
					+ "   ctx._source[\"incorrect\"] = ctx._source[\"incorrect\"] + params.incorrect;"
					+ "}" + "" + "if (!ctx._source.containsKey(\"answers\")) {"
					+ "	ctx._source[\"answers\"] = [params.answer];"
					+ "} else {"
					+ "   ctx._source[\"answers\"].add(params.answer)" + "}";
			update.script(new Script(ScriptType.INLINE, "painless", script,
					params));
			update.scriptedUpsert();
			update.retryOnConflict(50);
			hClient.update(update);

		} catch (Exception e) {
			LOG.error("updatePerformanceArray has failures : {}", e);
		}
	}

	public void updateCompletedArray(String overallIndex, String name,
			String target, float time, String activityId) {

		overallIndex += "-completable";
		try {
			GetRequest getRequest = new GetRequest(overallIndex,
					ESUtils.getResultsType(), "_average_completables");
			getRequest.fetchSourceContext(new FetchSourceContext(false));
			getRequest.storedFields("_none_");

			boolean exists = hClient.exists(getRequest);

			if (!exists) {
				IndexRequest indexRequest = new IndexRequest(overallIndex,
						ESUtils.getResultsType(), "_average_completables");
				indexRequest.source(new HashMap());
				try {
					hClient.index(indexRequest);
				} catch (Exception exp) {
					LOG.info("Not created index");
					exp.printStackTrace();
				}
			}

			UpdateRequest update = new UpdateRequest(overallIndex,
					ESUtils.getResultsType(), "_average_completables");

			if (target == null) {
				target = activityId;
			}

			Map params = new HashMap();
			params.put("target", target);
			Map comp = new HashMap();
			comp.put("n", 1);
			comp.put("time", time);
			params.put("comp", comp);
			params.put("name", name);

			String script = ""
					+ "if (!ctx._source.containsKey(params.target)) {"
					+ "	ctx._source[params.target] = params.comp;"
					+ "} else {"
					+ "   ctx._source[params.target].n = ctx._source[params.target].n + 1;"
					+ "   ctx._source[params.target].time = (ctx._source[params.target].time + params.comp.time) / ctx._source[params.target].n;"
					+ "}"
					+ "if(!ctx._source[params.target].containsKey(\"names\")) { "
					+ "   ctx._source[params.target].names = [params.name]; "
					+ "} else { "
					+ "   if(!ctx._source[params.target].names.contains(params.name)) {"
					+ "       ctx._source[params.target].names.add(params.name);"
					+ "   }" + "}";
			update.script(new Script(ScriptType.INLINE, "painless", script,
					params));
			update.detectNoop(false);
			update.retryOnConflict(50);
			update.fetchSource(true);
			UpdateResponse response = hClient.update(update);

			GetResult getResult = response.getGetResult();
			if (getResult.isExists()) {
				Map<String, Object> responseMap = getResult.sourceAsMap();

				Map targetMap = (Map) responseMap.get(target);
				List<String> names = (List) targetMap.get("names");

				for (int i = 0; i < names.size(); ++i) {
					String currName = names.get(i);

					if (currName.equalsIgnoreCase(name)) {

						GetRequest getRequest2 = new GetRequest(overallIndex,
								ESUtils.getResultsType(), currName);
						getRequest2.fetchSourceContext(new FetchSourceContext(
								false));
						getRequest2.storedFields("_none_");

						boolean exists2 = hClient.exists(getRequest2);

						if (!exists2) {
							IndexRequest indexRequest = new IndexRequest(
									overallIndex, ESUtils.getResultsType(),
									currName);
							indexRequest.source(new HashMap());
							try {
								hClient.index(indexRequest);
							} catch (Exception exp) {
								LOG.info("Not created index");
								exp.printStackTrace();
							}
						}

						UpdateRequest updatePlayer = new UpdateRequest(
								overallIndex, ESUtils.getResultsType(),
								currName);

						Map stuParams = new HashMap();
						Map stuRes = new HashMap();
						stuRes.put("mine", time);
						stuRes.put("n", 1);

						Map completable = new HashMap();
						completable.put("times", stuRes);

						Map finalResult = new HashMap();

						finalResult.put(target, completable);
						stuParams.put("finalResult", finalResult);
						stuParams.put("target", target);
						stuParams.put("resultMap", responseMap);

						script = ""
								+ "if (!ctx._source.containsKey(\"completables\")) {"
								+ "	ctx._source[\"completables\"] = params.finalResult;"
								+ "} else {"
								+ "   if (!ctx._source.completables.containsKey(params.target)) {"
								+ "	    ctx._source.completables[params.target] = params.finalResult[params.target];"
								+ "   } else {"
								+ "      ctx._source.completables[params.target].times.n = ctx._source.completables[params.target].times.n + 1;"
								+ "      ctx._source.completables[params.target].times.mine = (ctx._source.completables[params.target].times.mine + params.finalResult[params.target].times.mine) / ctx._source.completables[params.target].times.n;"
								+ "   }"
								+ "}"
								+ "def mAvg = 0;"
								+ "def allAvg = 0;"
								+ "def total = 0;"
								+ "for(entry in ctx._source.completables.entrySet()) {"
								+ "   total = total + 1;"
								+ "   mAvg = mAvg + entry.getValue().times.mine;"
								+ "   allAvg = allAvg + params.resultMap[entry.getKey()].time;"
								+ "}" + "ctx._source.mine = mAvg / total;"
								+ "ctx._source.avg = allAvg / total;" + "";
						updatePlayer.script(new Script(ScriptType.INLINE,
								"painless", script, stuParams));
						updatePlayer.scriptedUpsert();
						updatePlayer.retryOnConflict(50);
						hClient.update(updatePlayer);
					} else {
						GetRequest getRequest2 = new GetRequest(overallIndex,
								ESUtils.getResultsType(), currName);
						getRequest2.fetchSourceContext(new FetchSourceContext(
								false));
						getRequest2.storedFields("_none_");

						boolean exists2 = hClient.exists(getRequest2);

						if (!exists2) {
							IndexRequest indexRequest = new IndexRequest(
									overallIndex, ESUtils.getResultsType(),
									currName);
							indexRequest.source(new HashMap());
							try {
								hClient.index(indexRequest);
							} catch (Exception exp) {
								LOG.info("Not created index");
								exp.printStackTrace();
							}
						}

						UpdateRequest updatePlayer = new UpdateRequest(
								overallIndex, ESUtils.getResultsType(),
								currName);
						Map stuParams = new HashMap();
						stuParams.put("resultMap", responseMap);

						script = ""
								+ "def mAvg = 0;"
								+ "def allAvg = 0;"
								+ "def total = 0;"
								+ "if(ctx._source.containsKey(\"completables\")) {"
								+ "   for(entry in ctx._source.completables.entrySet()) {"
								+ "       total = total + 1;"
								+ "       mAvg = mAvg + entry.getValue().times.mine;"
								+ "       allAvg = allAvg + params.resultMap[entry.getKey()].time;"
								+ "   }"
								+ "   ctx._source.mine = mAvg / total;"
								+ "   ctx._source.avg = allAvg / total;" + "}"
								+ "";
						updatePlayer.script(new Script(ScriptType.INLINE,
								"painless", script, stuParams));
						updatePlayer.scriptedUpsert();
						updatePlayer.retryOnConflict(50);
						hClient.update(updatePlayer);
					}
				}
			}

		} catch (Exception e) {
			LOG.error("updatePerformanceArray has failures : {}", e);
			e.printStackTrace();
		}
	}

	public void updateProgressedArray(String overallIndex, String name,
			String target, float progress, String activityId) {

		overallIndex += "-progress";
		try {
			GetRequest getRequest = new GetRequest(overallIndex,
					ESUtils.getResultsType(), name);
			getRequest.fetchSourceContext(new FetchSourceContext(false));
			getRequest.storedFields("_none_");

			boolean exists = hClient.exists(getRequest);

			if (!exists) {
				IndexRequest indexRequest = new IndexRequest(overallIndex,
						ESUtils.getResultsType(), name);
				indexRequest.source(new HashMap());
				try {
					hClient.index(indexRequest);
				} catch (Exception exp) {
					LOG.info("Not created index");
					exp.printStackTrace();
				}
			}

			if (target == null) {
				target = activityId;
			}

			UpdateRequest updatePlayer = new UpdateRequest(overallIndex,
					ESUtils.getResultsType(), name);

			Map stuRes = new HashMap();
			stuRes.put("progress", progress);
			stuRes.put("n", 1);

			Map progressMap = new HashMap();
			progressMap.put(target, stuRes);

			Map stuParams = new HashMap();
			stuParams.put("finalResult", progressMap);
			stuParams.put("target", target);
			String script = ""
					+ "if (!ctx._source.containsKey(\"progress\")) {"
					+ "	ctx._source[\"progress\"] = params.finalResult;"
					+ "} else {"
					+ "   if (!ctx._source.progress.containsKey(params.target)) {"
					+ "	    ctx._source.progress[params.target] = params.finalResult[params.target];"
					+ "   } else {"
					+ "      ctx._source.progress[params.target].progress = params.finalResult[params.target].progress;"
					+ "   }" + "}" + "def mAvg = 0;" + "def total = 0;"
					+ "for(entry in ctx._source.progress.entrySet()) {"
					+ "   total = total + 1;"
					+ "   mAvg = mAvg + entry.getValue().progress;" + "}"
					+ "ctx._source.myProgress = mAvg / total;" + "";
			updatePlayer.script(new Script(ScriptType.INLINE, "painless",
					script, stuParams));
			updatePlayer.scriptedUpsert();
			updatePlayer.retryOnConflict(50);
			hClient.update(updatePlayer);

		} catch (Exception e) {
			LOG.error("updatePerformanceArray has failures : {}", e);
			e.printStackTrace();
		}
	}

	public Map<String, Object> getFromIndex(String index, String type, String id) {
		Map ret;
		try {
			GetRequest getRequest = new GetRequest(index, type, id);

			GetResponse resp = hClient.get(getRequest);

			ret = resp.getSourceAsMap();
		} catch (Exception e) {
			LOG.error(
					"error while executing getFromIndex request from elasticsearch, "
							+ "failed to store data into elasticsearch", e);
			ret = null;
		}
		return ret;
	}

	public void setOnIndex(String index, String type, String id, Map source) {
		try {

			hClient.update(new UpdateRequest(index, type, id).docAsUpsert(true)
					.doc(source).retryOnConflict(50));
		} catch (Exception e) {
			LOG.error(
					"error while executing setOnIndex request to elasticsearch, "
							+ "failed to store data into elasticsearch", e);

		}
	}

	public static StateFactory opaque() {
		return new Factory();
	}

	public static class Factory implements StateFactory {

		public Factory() {
		}

		@Override
		public State makeState(Map conf, IMetricsContext context,
				int partitionIndex, int numPartitions) {

			String esHost = (String) conf
					.get(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM);
			RestClient client = makeElasticsearchClient(new HttpHost(esHost,
					9200));
			EsState s = new EsState(client, new RestHighLevelClient(client));

			return s;
		}

		/**
		 * Constructs a java elasticsearch 5 client for the host..
		 * 
		 * @return {@link RestClient} to read/write to the hash ring of the
		 *         servers..
		 */
		public RestClient makeElasticsearchClient(HttpHost... endpoints) {
			return RestClient.builder(endpoints).build();
		}
	}
}
