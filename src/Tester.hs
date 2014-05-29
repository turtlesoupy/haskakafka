module Main (main) where

import Haskakafka
import Haskakafka.Internal

import System.IO

main :: IO ()
main = do
    kConf <- newKafkaConf
    conf <- dumpKafkaConf kConf

    kTopicConf <- newKafkaTopicConf
    tConf <- dumpKafkaTopicConf kTopicConf

    kafka <- newKafka RdKafkaConsumer kConf
    addBrokers kafka "localhost:9092"
    topic <- newKafkaTopic kafka "topic" kTopicConf

    startConsuming topic 0 0
    m <- consumeMessage topic 0 1000
    print m
    stopConsuming topic 0

    -- hPrintKafkaProperties stdout
    --o <- c_stdout
    --rdKafkaConfPropertiesShow o
    --putStrLn $ "Kafka version: " ++ rdKafkaVersionStr
    --t <- c_rd_kafka_topic_conf_new
    --topics <- dumpTopics t
    --print topics
    --c_rd_kafka_topic_conf_destroy t
    --putStrLn $ "Cleaned up"
