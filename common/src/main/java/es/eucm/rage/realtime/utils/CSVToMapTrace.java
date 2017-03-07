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
package es.eucm.rage.realtime.utils;

import es.eucm.rage.realtime.topologies.TopologyBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class CSVToMapTrace {

	private String sessionId;

	public CSVToMapTrace(String sessionId) {
		this.sessionId = sessionId;
	}

	public List<List<Object>> getTuples(String csvTracesFile) {

		List<List<Object>> ret = new ArrayList<List<Object>>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream(csvTracesFile)));

		String line;
		try {
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("--") && line.length() > 10) {
					Map trace = CreateStatement(line);
					if (trace != null) {
						trace.put("gameplayId", csvTracesFile);
						ret.add(Arrays.asList(sessionId, trace));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	private Map<String, Object> CreateStatement(String trace) {
		List<String> p = new ArrayList<String>();

		String[] parts = trace.split(",");
		String timestamp = parts[0];

		Map<String, Object> ret = new HashMap<String, Object>();

		ret.put(TopologyBuilder.TridentTraceKeys.TIMESTAMP, timestamp);

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
					} else {
						extensions.put(key, value);
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
}
