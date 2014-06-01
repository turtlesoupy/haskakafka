module Main (main) where

import Haskakafka

import Control.Monad
import Control.Exception (evaluate)

import qualified Data.ByteString.Char8 as BS

doConsume :: IO ()
doConsume = do
    kConf <- newKafkaConf
    kafka <- newKafka KafkaConsumer kConf
    addBrokers kafka "localhost:9092"
    kTopicConf <- newKafkaTopicConf
    topic <- newKafkaTopic kafka "test" kTopicConf

    startConsuming topic 0 (KafkaOffsetBeginning)
    _ <- forever $ do
          m <- consumeMessage topic 0 (1000 * 1000)
          print m
    stopConsuming topic 0

doProduce :: IO ()
doProduce = do
    kConf <- newKafkaConf
    kafka <- newKafka KafkaProducer kConf
    addBrokers kafka "localhost:9092"
    kTopicConf <- newKafkaTopicConf
    topic <- newKafkaTopic kafka "test" kTopicConf
    let me = KafkaMessage 0 0 (BS.pack "hi") Nothing
    err <- produceMessage topic me

    drainOutQueue kafka
            
    print err


main :: IO ()
main = do
    -- doProduce
    -- doConsume

    kConf <- newKafkaConf
    kafka <- newKafka KafkaConsumer kConf
    addBrokers kafka "localhost:9092"

    kTopicConf <- newKafkaTopicConf
    topic <- newKafkaTopic kafka "test" kTopicConf

    getAllMetadata kafka 1000 >>= print
    putStrLn "TOPIC MD"
    getTopicMetadata kafka topic 1000 >>= print

    -- hPrintKafkaProperties stdout
    --o <- c_stdout
    --rdKafkaConfPropertiesShow o
    --putStrLn $ "Kafka version: " ++ rdKafkaVersionStr
    --t <- c_rd_kafka_topic_conf_new
    --topics <- dumpTopics t
    --print topics
    --c_rd_kafka_topic_conf_destroy t
    --putStrLn $ "Cleaned up"
  
