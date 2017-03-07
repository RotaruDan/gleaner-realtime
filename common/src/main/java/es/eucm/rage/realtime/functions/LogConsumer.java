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
package es.eucm.rage.realtime.functions;

import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConsumer implements Consumer {

	public static boolean LOG_ENABLED = false;

	private static final Logger LOG = LoggerFactory
			.getLogger(LogConsumer.class);

	private String prefix = "";

	public LogConsumer() {
	}

	public LogConsumer(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public void accept(TridentTuple tridentTuple) {
		if (LOG_ENABLED) {
			LOG.info(prefix + " - " + tridentTuple.toString());
		}
	}
}
