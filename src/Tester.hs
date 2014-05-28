module Main (main) where

import Haskakafka
import Haskakafka.Internal

import System.IO

main :: IO ()
main = do
    kConf <- newKafkaConf
    conf <- dumpKafkaConf kConf
    print conf

    kTopicConf <- newKafkaTopicConf
    tConf <- dumpKafkaTopicConf kTopicConf
    print tConf

    kafka <- newKafka RdKafkaConsumer kConf
    hPrintKafka stderr kafka
    addBrokers kafka "localhost:9092"

    -- hPrintKafkaProperties stdout
    --o <- c_stdout
    --rdKafkaConfPropertiesShow o
    --putStrLn $ "Kafka version: " ++ rdKafkaVersionStr
    --t <- c_rd_kafka_topic_conf_new
    --topics <- dumpTopics t
    --print topics
    --c_rd_kafka_topic_conf_destroy t
    --putStrLn $ "Cleaned up"
