#!/bin/bash
for KILLPID in `ps ax | grep 'start-safers-service' | awk '{print $1;}'`; do
 kill -9 $KILLPID;
done
