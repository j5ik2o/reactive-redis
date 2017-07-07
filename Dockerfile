FROM ubuntu:14.04


ENV JAVA_HOME "/usr/lib/jvm/java-8-oracle"

# Install Curl
RUN apt-get update -y && apt-get install -y curl

# Install java
RUN \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 \
    && echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list \
    && echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list \
    && echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections \
    && apt-get update -y \
    && apt-get install -y oracle-java8-installer

# Install SBT
RUN \
    echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && apt-get update -y \
    && apt-get install -y sbt \
  && sbt sbtVersion

RUN sudo apt-get update -qq && sudo apt-get install -y redis-server

RUN mkdir -p /tmp/work
