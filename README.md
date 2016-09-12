# Analytics Realtime

[![Build Status](https://travis-ci.org/e-ucm/rage-analytics-realtime.svg)](https://travis-ci.org/e-ucm/rage-analytics-realtime) [![Coverage Status](https://coveralls.io/repos/e-ucm/rage-analytics-realtime/badge.svg?branch=master&service=github)](https://coveralls.io/github/e-ucm/rage-analytics-realtime?branch=master)

![general architecture](https://cloud.githubusercontent.com/assets/2271676/18433512/47d0b368-78e8-11e6-815e-34dd57dd5a9e.png)

This is the default analysis that gets executed for analytics traces from games. The Analytics Back-end can use any other analysis that can read fro the input kafka-queue in the required format. This documentation is therefore useful both to understand the Analytics Realtime code, and to build your own analysis from scratch.

## Basic requirements

Your analysis must mimic the signature of the RealTime class, by providing methods to return a suitable StormTopology as a response to a suitable config.

## Analysis input

## Analysis output
