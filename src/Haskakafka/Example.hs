module Haskakafka.Example where

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
