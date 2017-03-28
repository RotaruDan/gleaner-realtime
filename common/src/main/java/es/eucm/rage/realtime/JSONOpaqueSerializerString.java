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

import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.trident.state.OpaqueValue;
import org.json.simple.JSONObject;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JSONOpaqueSerializerString {
	private static final class MapOpaqueValue {
		long t;
		Object c, p;
	}

	public JSONOpaqueSerializerString() {
	}

	public String serialize(OpaqueValue<Object> obj) {

		String ret;
		try {
			ret = jsonBuilder().startObject().field("t", obj.getCurrTxid())
					.field("c", obj.getCurr()).field("p", obj.getPrev())
					.endObject().string();
		} catch (IOException e) {
			ret = "{" + "\"t\":\"" + obj.getCurrTxid() + "\"," + "\"c\":\""
					+ obj.getCurr() + "\"," + "\"p\":\"" + obj.getPrev() + "\""
					+ "}";
		}

		return ret;
	}

	public OpaqueValue<Object> deserialize(String b) {
		Object opaqueValueObject = JSONValue.parse(b);
		if (opaqueValueObject instanceof MapOpaqueValue) {

			MapOpaqueValue res = (MapOpaqueValue) JSONValue.parse(b);
			return new OpaqueValue<Object>(res.t, res.c, res.p);
		} else if (opaqueValueObject instanceof JSONObject) {
			JSONObject opaqueValueJson = (JSONObject) opaqueValueObject;

			return new OpaqueValue<Object>(Long.valueOf(opaqueValueJson
					.getOrDefault("t", "0").toString()),
					opaqueValueJson.get("c"), opaqueValueJson.get("p"));
		} else {
			return new OpaqueValue<Object>(0l, null, null);
		}

	}
}
