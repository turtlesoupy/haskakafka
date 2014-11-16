# Haskakafka

Kafka bindings for Haskell backed by the 
librdkafka C module (https://github.com/edenhill/librdkafka). It has been tested and fully
supports Kafka 0.8.x using librdkafka 0.8.1 and higher on Linux and OS X. Haskakafka supports
keyed/unkeyed producers and consumers with optional batch operations. 

# Usage 


## Configuration Options
Configuration options are set in the call to `withKafkaConsumer` and `withKafkaProducer`. For
the full list of supported options, see 
[librdkafka's list](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

# Installation

## Installing librdkafka

Although librdkafka is available on many platforms, most of
the distribution packages are too old to support haskakafka.
As such, we suggest you install from the source:

    git clone https://github.com/edenhill/librdkafka
    cd librdkafka
    ./configure
    make && sudo make install

On OSX, the C++ bindings were failing for me. If this is the case, just install the C bindings alone. 

    cd librdkafka/src
    make && sudo make install

## Installing Kafka

The full Kafka guide is at http://kafka.apache.org/documentation.html#quickstart

## Installing Haskakafka

Since haskakafka uses `c2hs` to generate C bindings, you may need to 
explicitly install `c2hs` somewhere on your path (i.e. outside of a sandbox).
To do so, run:
    
    cabal install c2hs

Afterwards installation should work, so go for

    cabal install haskakafka

# Testing

Haskakafka ships with a suite of integration tests to verify the library against
a live Kafka instance. To get these setup you must have a broker running
on `localhost:9092` (or overwrite the `HASKAKAFKA_TEST_BROKER` environment variable)
with a `haskakafka_tests` topic created (or overwrite the `HASKAKAFKA_TEST_TOPIC` 
environment variable).

To get a broker running, download a [Kafka distribution](http://kafka.apache.org/downloads.html)
and untar it into a directory. From there, run zookeeper using
  
    bin/zookeeper-server-start.sh config/zookeeper.properties

and run kafka in a separate window using
  
    bin/kafka-server-start.sh config/server.properties

With both Kafka and Zookeeper running, you can run tests through cabal:
  
    cabal install --only-dependencies --enable-tests
    cabal test --log=/dev/stdout
