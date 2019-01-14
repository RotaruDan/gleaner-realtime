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
package es.eucm.rage.realtime.functions;

import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple LOGGING function that, if activated will prompt the value of the
 * touples that pass by.
 */
public class LogConsumer implements Consumer {

	public static boolean LOG_ENABLED = false;

	private static final Logger LOG = LoggerFactory
			.getLogger(LogConsumer.class);

	private String prefix = "";
	private boolean log = false;

	/**
	 * LOGGER is desactivated, will add at the beginning of the log the "prefix"
	 * parameter.
	 * 
	 * @param prefix
	 */
	public LogConsumer(String prefix) {
		this(prefix, false);
	}

	/**
	 * Creates a simple LOGGER for the Storm Trident pipeline.
	 * 
	 * @param prefix
	 *            String to add at the beginning of the log.
	 * @param log
	 *            wether the log is activated or not.
	 */
	public LogConsumer(String prefix, boolean log) {
		this.prefix = prefix;
		this.log = log;
	}

	@Override
	public void accept(TridentTuple tridentTuple) {
		try {
			if (LOG_ENABLED || log) {
				LOG.info(prefix + " - " + tridentTuple.toString());
			}
		} catch (Exception ex) {
			LOG.info("Error unexpected exception, discarding" + ex.toString());
		}
	}
}
