#!/bin/bash

# @pathChange to the path that has your directory with all the files to stream.
for file in /mnt/f/Users/ismer/Documents/CSD/ptuxiaki/twitter\ live\ stream/src/main/resources/data/*; do
	  echo "Now reading ${file##*/}"
	  $(./kafka_2.13-2.8.0/bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092 < "${file}")
	sleep 6;
  done
