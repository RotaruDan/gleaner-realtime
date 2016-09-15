# Analytics Realtime

[![Build Status](https://travis-ci.org/e-ucm/rage-analytics-realtime.svg)](https://travis-ci.org/e-ucm/rage-analytics-realtime) [![Coverage Status](https://coveralls.io/repos/e-ucm/rage-analytics-realtime/badge.svg?branch=master&service=github)](https://coveralls.io/github/e-ucm/rage-analytics-realtime?branch=master)

![general architecture](https://cloud.githubusercontent.com/assets/2271676/18433512/47d0b368-78e8-11e6-815e-34dd57dd5a9e.png)
(Fig. 1: General architecture. This project only provides the yellow box; the rest is part of the [Rage Analytics](https://github.com/e-ucm/rage-analytics) platform)

This is the default analysis that gets executed for analytics traces from games. The Analytics Back-end can use any other analysis that can read from the input kafka-queue in the required format. This documentation is therefore useful both to understand the Analytics Realtime code, and to build your own analysis from scratch.

## Basic requirements

Your analysis must mimic the signature of the [RealTime](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/RealTime.java) class, by providing methods to return a suitable StormTopology as a response to a suitable config. As can be seen in Fig. 1, the analysis will be run within Apache Trident.

The project can be tested outside this architecture by using the built-in tests (via `mvn test`, assuming you have Maven correctly installed.)

## Analysis input

Incoming tuples will be of the form `versionId, Map<String, Object>`. `versionId`s track particular instances of games being played; for example, all students in a class could would share the same `versionId`; but if the game were to be played later, the teacher would typically generate another `versionId`. Map keys are generally of the form 
Which is derived by the Analytics Backend in several steps
* [From xAPI to simplified JSON](https://github.com/e-ucm/rage-analytics-backend/blob/master/lib/tracesConverter.js#L184), which is then sent to a Kafka queue. The queue provides a buffer against loss of traces if the analysis cannot keep up with a spike in trace activity.
* From Kafka to into your analysis: [Extraction from Kafka](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/topologies/KafkaTopology.java#L52), and [conversion into final format](https://github.com/e-ucm/rage-analytics-realtime/blob/master/src/main/java/es/eucm/rage/realtime/functions/JsonToTrace.java#L42). Note that you could choose to reimplement these differently, as they are both part of this module.

Sample processed traces could be (with columns representing `versionId`, `gameplayId`, `event`, `target`, and `value`, respectively; in the first trace, target is not set). The following example would indicate a player 14 moving into `zone3`, and setting var `var1` to the value `1`:

    23,14,zone,,zone3
    23,14,var,var1,1

xAPI traces sent by games should comply with the [xAPI for serious games specification](https://github.com/e-ucm/xapi-seriousgames).


## Analysis output

The analysis will be provided with details on how to connect to MongoDB and ElasticSearch back-ends. Currently, the same information is stored on both, although we are considering *deprecating the MongoDB* back-end, as only ElasticSearch is currently used in visualizations (via Kibana).


