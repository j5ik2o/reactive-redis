#!/bin/sh

# sbt 'benchmark/jmh:run -prof jmh.extras.JFR:dir=.. -i 5 -wi 3 -f1 -t1'
sbt 'benchmark/jmh:run -i 5 -wi 3 -f1 -t1'
