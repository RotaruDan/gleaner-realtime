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
package es.eucm.rage.realtime.utils;

import es.eucm.rage.realtime.topologies.TopologyBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class CSVToMapTrace {

	int biasesCount = 0;

	public CSVToMapTrace() {
	}

	public List<List<Object>> getTuples(String csvTracesFile,
			String activityId, int i) {
		biasesCount = 0;
		List<List<Object>> ret = new ArrayList<List<Object>>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream(csvTracesFile)));

		String line;
		try {
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("--") && line.length() > 10) {
					Map outTrace = CreateStatement(line);
					if (outTrace != null) {

						Map trace = new HashMap<String, Object>();
						trace.put(TopologyBuilder.GAMEPLAY_ID, "gameplayid" + i);
						trace.put(TopologyBuilder.OUT_KEY, outTrace);
						trace.put(TopologyBuilder.ACTIVITY_ID_KEY, activityId);
						trace.put(TopologyBuilder.UUIDV4, UUID.randomUUID()
								.toString());
						ret.add(Arrays.asList(trace));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	private Map<String, Object> CreateStatement(String trace) {

		String[] parts = trace.split(",");
		String timestamp = parts[0];

		Map<String, Object> ret = new HashMap<String, Object>();

		ret.put(TopologyBuilder.TridentTraceKeys.TIMESTAMP,
				new Date(Long.valueOf(timestamp)));

		ret.put(TopologyBuilder.TridentTraceKeys.EVENT, parts[1]);

		ret.put(TopologyBuilder.TridentTraceKeys.TYPE, parts[2]);
		ret.put(TopologyBuilder.TridentTraceKeys.TARGET, parts[3]);

		if (parts.length > 4) {
			// Parse extensions

			int extCount = parts.length - 4;
			if (extCount > 0 && extCount % 2 == 0) {
				// Extensions come in <key, value> pairs

				Map<String, Object> extensions = new HashMap<String, Object>();
				for (int i = 4; i < parts.length; i += 2) {
					String key = parts[i];
					String value = parts[i + 1];
					if (key.equals("") || value.equals("")) {
						continue;
					}
					value = value.toString();
					if (key.equalsIgnoreCase(TopologyBuilder.TridentTraceKeys.SCORE)) {
						ret.put(key, value);

					} else if (key
							.equalsIgnoreCase(TopologyBuilder.TridentTraceKeys.SUCCESS)) {
						ret.put(key, value);
					} else if (key.equalsIgnoreCase("completion")) {
						ret.put(key, value);
					} else if (key
							.equalsIgnoreCase(TopologyBuilder.TridentTraceKeys.RESPONSE)) {
						ret.put(key, value);
					} else if (key
							.equalsIgnoreCase(TopologyBuilder.TridentTraceKeys.NAME)) {
						ret.put(key, value);
					} else if (key.equalsIgnoreCase("biases")) {
						if (ret.get(TopologyBuilder.TridentTraceKeys.EVENT)
								.equals(TopologyBuilder.TraceEventTypes.SELECTED)) {

							String[] biases = value.split("-");

							Map<String, Object> biasesObject = new HashMap<>(
									biases.length);

							if(parts[1].equalsIgnoreCase("selected")) {
								biasesCount += biases.length;
							}
							for (int k = 0; k < biases.length; ++k) {
								String[] bias = biases[k].split("=");
								if (bias[1].startsWith("random")) {
									biasesObject.put(bias[0],
											Math.random() > .5d ? true : false);
								} else {
									biasesObject.put(bias[0],
											Boolean.valueOf(bias[1]));
								}
							}
							extensions.put(key, biasesObject);
						}
					} else if (key.equalsIgnoreCase("thomasKilmann")) {
						if (ret.get(TopologyBuilder.TridentTraceKeys.EVENT)
								.equals(TopologyBuilder.TraceEventTypes.SELECTED)) {

							if (!value.contains("random")) {
								extensions.put(key, value);
							} else {
								String[] thomasKilmannValues = value.split("=")[1]
										.split("-");
								extensions
										.put(key,
												thomasKilmannValues[new Random()
														.nextInt(thomasKilmannValues.length)]);
							}
						}
					} else {

						try {
							int valI = Integer.valueOf(value);
							extensions.put(key, valI);
						} catch (Exception ex) {
							try {
								float valF = Float.valueOf(value);
								extensions.put(key, valF);
							} catch (Exception exF) {
								try {
									double valD = Double.valueOf(value);
									extensions.put(key, valD);
								} catch (Exception exD) {
									extensions.put(key, value);
								}
							}
						}
					}
				}

				ret.put("ext", extensions);
			}
		}

		return ret;
	}

	public List<String> getLines(String file) {
		List<String> ret = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream(file)));

		String line;
		try {
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("--") && line.length() > 10) {
					ret.add(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	public int getBiasesCount() {
		return biasesCount;
	}
}
