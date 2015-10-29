#!/bin/sh

nohup /usr/local/tibco_be/tibcojre64/1.7.0/bin/java -classpath bejmx-2.3.jar com.tibco.metrics.bejmx.BEJMX -config config.properties &

# /Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/bin/java -classpath bejmx-2.3.jar:tools.jar com.tibco.metrics.bejmx.BEJMX -config config.properties -pid 1251