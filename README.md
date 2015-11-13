# Haskakafka 

Kafka bindings for Haskell backed by the 
[librdkafka C module](https://github.com/edenhill/librdkafka). It has been tested and fully
supports Kafka 0.8.x using librdkafka 0.8.1 and higher on Linux and OS X. Haskakafka supports
both producers and consumers with optional batch operations. 

Hackage: http://hackage.haskell.org/package/haskakafka

# Usage 
A quick walkthrough of producers and consumers:
```Haskell
import Haskakafka

import qualified Data.ByteString.Char8 as C8

example :: IO ()
example = do
  let 
      -- Optionally, we can configure certain parameters for Kafka
      kafkaConfig = [("socket.timeout.ms", "50000")]
      topicConfig = [("request.timeout.ms", "50000")]

      -- Payloads are just ByteStrings
      samplePayload = C8.pack "Hello world"


  -- withKafkaProducer opens a producer connection and gives us 
  -- two objects for subsequent use.
  withKafkaProducer kafkaConfig topicConfig 
                    "localhost:9092" "test_topic" 
                    $ \kafka topic -> do

    -- Produce a single unkeyed message to partition 0
    let message = KafkaProduceMessage samplePayload
    _ <- produceMessage topic (KafkaSpecifiedPartition 0) message

    -- Produce a single keyed message
    let keyMessage = KafkaProduceKeyedMessage (C8.pack "Key") samplePayload
    _ <- produceKeyedMessage topic keyMessage

    -- We can also use the batch API for better performance
    _ <- produceMessageBatch topic KafkaUnassignedPartition [message, keyMessage]

    putStrLn "Done producing messages, here was our config: "
    dumpConfFromKafka kafka >>= \d -> putStrLn $ "Kafka config: " ++ (show d)
    dumpConfFromKafkaTopic topic >>= \d -> putStrLn $ "Topic config: " ++ (show d)


  -- withKafkaConsumer opens a consumer connection and starts consuming
  let partition = 0
  withKafkaConsumer kafkaConfig topicConfig 
                    "localhost:9092" "test_topic"
                    partition -- locked to a specific partition for each consumer
                    KafkaOffsetBeginning -- start reading from beginning (alternatively, use
                                         -- KafkaOffsetEnd, KafkaOffset or KafkaOffsetStored)
                    $ \kafka topic -> do
    -- Consume a single message at a time
    let timeoutMs = 1000
    me <- consumeMessage topic partition timeoutMs
    case me of 
      (Left err) -> putStrLn $ "Uh oh, an error! " ++ (show err)
      (Right m) -> putStrLn $ "Woo, payload was " ++ (C8.unpack $ messagePayload m)

    -- For better performance, consume in batches
    let maxMessages = 10
    mes <- consumeMessageBatch topic partition timeoutMs maxMessages
    case mes of 
      (Left err) -> putStrLn $ "Something went wrong in batch consume! " ++ (show err)
      (Right ms) -> putStrLn $ "Woohoo, we got " ++ (show $ length ms) ++ " messages"


    -- Be a little less noisy
    setLogLevel kafka KafkaLogCrit

  -- we can also fetch metadata about our Kafka infrastructure
  let timeoutMs = 1000
  emd <- fetchBrokerMetadata [] "localhost:9092" timeoutMs
  case emd of 
    (Left err) -> putStrLn $ "Uh oh, error time: " ++ (show err)
    (Right md) -> putStrLn $ "Kafka metadata: " ++ (show md)
```

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

This uses the latest version of Haskakafka from [Hackage](http://hackage.haskell.org/package/haskakafka)

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
