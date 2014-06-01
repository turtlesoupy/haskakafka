module Main (testmain, main) where

import Haskakafka

import Control.Monad
import Test.Hspec
import Control.Exception (evaluate)

import qualified Data.ByteString.Char8 as BS

testmain :: IO ()
testmain = hspec $ do
  describe "Prelude.head" $ do
    it "returns the first element of a list" $ do
      head [23 ..] `shouldBe` (23 :: Int)

    it "throws an exception if used with an empty list" $ do
      evaluate (head []) `shouldThrow` anyException

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
    doProduce
    doConsume


    -- hPrintKafkaProperties stdout
    --o <- c_stdout
    --rdKafkaConfPropertiesShow o
    --putStrLn $ "Kafka version: " ++ rdKafkaVersionStr
    --t <- c_rd_kafka_topic_conf_new
    --topics <- dumpTopics t
    --print topics
    --c_rd_kafka_topic_conf_destroy t
    --putStrLn $ "Cleaned up"
  
