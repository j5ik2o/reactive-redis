#!/bin/sh

docker run -i -t -v $(pwd):/tmp/work sbt-test /bin/sh -c "cd /tmp/work && sbt test"
