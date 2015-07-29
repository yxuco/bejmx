#!/bin/sh

nohup /usr/local/tibco_be/tibcojre64/1.7.0/bin/java -classpath bejmx-2.1.jar com.tibco.metrics.bejmx.BEJMX -config config.properties &

