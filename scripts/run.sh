#!/bin/bash

java --add-opens=java.base/java.lang=ALL-UNNAMED -jar ../target/testFlink-1.0-SNAPSHOT.jar > ../log/run.log 2>&1