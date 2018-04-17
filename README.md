# Analytics Realtime

[![Build Status](https://travis-ci.org/e-ucm/rage-analytics-realtime.svg)](https://travis-ci.org/e-ucm/rage-analytics-realtime) [![Coverage Status](https://coveralls.io/repos/e-ucm/rage-analytics-realtime/badge.svg?branch=master&service=github)](https://coveralls.io/github/e-ucm/rage-analytics-realtime?branch=master)

![architecture-3-style-unified-and-updated](https://cloud.githubusercontent.com/assets/19714314/19108724/301c3a00-8af2-11e6-9762-a53660c0594f.png)
(Fig. 1: General architecture. This project only provides the Real-time analysis box; the rest is part of the [Rage Analytics](https://github.com/e-ucm/rage-analytics) platform)

This is the default analysis that gets executed for analytics traces from games. The Analytics Back-end can use any other analysis that can read from the input kafka-queue in the required format. This documentation is therefore useful both to understand the Analytics Realtime code, and to build your own analysis from scratch. For a detailed information about the data flow check out [Understanding RAGE Analytics Traces Flow](https://github.com/e-ucm/rage-analytics/wiki/Understanding-RAGE-Analytics-Traces-Flow).

## Basic requirements

Your analysis must mimic the signature of the [RealTime](https://github.com/e-ucm/rage-analytics-realtime/blob/master/default/src/main/java/es/eucm/rage/realtime/simple/Analysis.java) class, by providing methods to return a suitable StormTopology as a response to a suitable config. As can be seen in Figure 1, the analysis will run within Apache Trident.

The project can be tested outside this architecture by using the built-in tests (via `mvn test`, assuming you have Maven correctly installed).

## Analysis input

Incoming tuples will be of the form `versionId, Map<String, Object>`. `versionId`s track particular instances of games being played; for example, all students in a class could share the same `versionId`, but if the game were to be played later, the teacher would typically generate another `versionId`. Map keys are generally of the form derived by the Analytics Back-end in several steps:
* [From xAPI to simplified JSON](https://github.com/e-ucm/rage-analytics-backend/blob/master/lib/tracesConverter.js#L184), which is then sent to a Kafka queue. The queue provides a buffer to prevent the loss of traces if the analysis cannot keep up with a spike in trace activity. For a full understanding of the input format of the data from Kafka check out [Understanding RAGE Analytics Traces Flow - Step 2 - Collector (Backend) to Kibana then Storm Realtime](https://github.com/e-ucm/rage-analytics/wiki/Understanding-RAGE-Analytics-Traces-Flow#step-2---collector-to-real-time)
* From Kafka into your analysis: [Extraction from Kafka](https://github.com/e-ucm/rage-analytics-realtime/blob/master/default/src/main/java/es/eucm/rage/realtime/simple/topologies/TopologyBuilder.java#L38), and [conversion into final format](https://github.com/e-ucm/rage-analytics-realtime/blob/master/common/src/main/java/es/eucm/rage/realtime/functions/JsonToTrace.java#L43). Note that you could choose to reimplement these differently, as they are both part of this module. For a full understanding of how to connect with Kafka check out [Understanding RAGE Analytics Traces Flow - Step 3 - Realtime, transforming data from Kafka to ElasticSearch](https://github.com/e-ucm/rage-analytics/wiki/Understanding-RAGE-Analytics-Traces-Flow#step-3---real-time-from-kafka-to-elasticsearch)

Sample processed traces (with columns representing `versionId`, `gameplayId`, `event`, `target`, and `response`, respectively) could be the following:

    23,14,preferred,menu_start,tutorial_mode
    23,14,skipped,introvideo1
    
This example would indicate a player 14 has selected `menu_start` (type `alternative`), and skipped var `introvideo1 cutscene`. Also note that in the second trace, response is not set.

xAPI traces sent by games should comply with the [xAPI for serious games specification](https://github.com/e-ucm/xapi-seriousgames).


## Analysis output

The analysis will include details on how to connect to the ElasticSearch back-end. The information obtained from the analysis is stored in ElasticSearch for its use in visualizations (via Kibana). For a full understanding about the analysis output check out [Storm Trident Computation](https://github.com/e-ucm/rage-analytics/wiki/Understanding-RAGE-Analytics-Traces-Flow#storm-trident-computation).

## Useful Maven goals

- `mvn clean install`: run tests, check correct headers and generate `realtime-jar-with-dependencies.jar` file inside `/target` folder
- `mvn clean test`: run tests checking topology output
- `mvn license:check`: verify if some files miss license header. This goal is attached to the verify phase if declared in your pom.xml like above.
- `mvn license:format`: add the license header when missing. If a header is existing, it is updated to the new one.
- `mvn license:remove`: remove existing license header

## Key notes for Implementing a RAGE Analytics Realtime Analysis

This is a list of considerations before implementing a new RAGE Analytics Realtime file.

* The analysis package must follow the restrictions described [here](https://github.com/e-ucm/rage-analytics/wiki/Analysis-Configuration#the-analysis-package).
* Starting method of our topology class can be found [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/default/src/main/java/es/eucm/rage/realtime/simple/Analysis.java#L32).
* Example connecting to Kafka to pull data [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/KafkaTopology.java#L37).
* Converting String data from Kafka to [JSON](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/functions/JsonToTrace.java) objects [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/KafkaTopology.java#L48).
* ElasticSearch indices names definition [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/utils/DBUtils.java#L58), methods `getTracesIndex(String sessionId)` and `getResultsIndex(String sessionId)`.
* Topology definition (Storm Trident API) [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/RealtimeTopology.java#L41).
    * [Sanitizing traces](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/states/DocumentBuilder.java#L82) and [defining the analysis stream](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/RealtimeTopology.java#L48) and persisting it to [`sessionId`](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/states/ESGameplayState.java#L173) ElasticSearch index (traces index).
    * [Realtime GamePlay State](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/RealtimeTopology.java#L55) analysis definition and persisting to [`results-sessionId`](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/states/ESGameplayState.java#L69) ElasticSearch index (results index).
* Topology [tests example](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/test/java/es/eucm/rage/realtime/RealtimeTopologyTest.java).
* Project [dependencies](https://github.com/e-ucm/rage-analytics-realtime/blob/master/pom.xml#L37) assembled [here](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/assembly/jar.xml).
