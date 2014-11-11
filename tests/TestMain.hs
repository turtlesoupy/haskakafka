module Main (main) where
import Haskakafka

import Control.Concurrent
import Data.Either.Unwrap
import Test.Hspec
import Text.Regex.Posix

import qualified Data.Map as Map
import qualified Data.ByteString.Char8 as C8

brokerAddress :: String
brokerAddress = "localhost:9092"
brokerTopic :: String
brokerTopic = "haskakafka_tests"

testmain :: IO ()
testmain = hspec $ do
  describe "RdKafka versioning" $ do
    it "should be a valid version number" $ do
      rdKafkaVersionStr `shouldSatisfy` (=~"[0-9]+(.[0-9]+)+")

  describe "Supported properties" $ do
    it "should list supported properties" $ do
      props <- supportedKafkaConfProperties
      props `shouldSatisfy` (\x -> (length x) > 0)

  describe "Kafka Configuration" $ do
    it "should allow dumping" $ do
      kConf <- newKafkaConf 
      kvs <- dumpKafkaConf kConf
      (Map.size kvs) `shouldSatisfy` (>0)

    it "should change when set is called" $ do
      kConf <- newKafkaConf 
      setKafkaConfValue kConf "socket.timeout.ms" "50000"
      kvs <- dumpKafkaConf kConf
      (kvs Map.! "socket.timeout.ms") `shouldBe` "50000"

    it "should throw an exception on unknown property" $ do
      kConf <- newKafkaConf
      (setKafkaConfValue kConf "blippity.blop.cosby" "120") `shouldThrow`
        (\(KafkaUnknownConfigurationKey str) -> (length str) > 0)

    it "should throw an exception on an invalid value" $ do
      kConf <- newKafkaConf
      (setKafkaConfValue kConf "socket.timeout.ms" "monorail") `shouldThrow`
        (\(KafkaInvalidConfigurationValue str) -> (length str) > 0)
  
  describe "Kafka topic configuration" $ do
    it "should allow dumping" $ do
      kConf <- newKafkaTopicConf
      kvs <- dumpKafkaTopicConf kConf
      (Map.size kvs) `shouldSatisfy` (>0)

    it "should change when set is called" $ do
      kConf <- newKafkaTopicConf
      setKafkaTopicConfValue kConf "request.timeout.ms" "20000"
      kvs <- dumpKafkaTopicConf kConf
      (kvs Map.! "request.timeout.ms") `shouldBe` "20000"

    it "should throw an exception on unknown property" $ do
      kConf <- newKafkaTopicConf
      (setKafkaTopicConfValue kConf "blippity.blop.cosby" "120") `shouldThrow`
        (\(KafkaUnknownConfigurationKey str) -> (length str) > 0)

    it "should throw an exception on an invalid value" $ do
      kConf <- newKafkaTopicConf
      (setKafkaTopicConfValue kConf "request.timeout.ms" "mono...doh!") `shouldThrow`
        (\(KafkaInvalidConfigurationValue str) -> (length str) > 0)

  describe "Consume and produce cycle" $ do
    it "should be able to produce and consume a unkeyed message off of the broker" $ do

      let message = KafkaProduceMessage (C8.pack "hey hey we're the monkeys")
      withKafkaConsumer [] [] brokerAddress brokerTopic 0 KafkaOffsetEnd $ \_ topic -> do
        withKafkaProducer [] [] brokerAddress brokerTopic $ \_ producerTopic -> do 
          produceMessage producerTopic (KafkaSpecifiedPartition 0) message
        
        threadDelay $ 2500 * 1000

        et <- consumeMessage topic 0 (1000 * 1000)

        (messagePayload $ fromRight et) `shouldBe` (C8.pack "hey hey we're the monkeys")
        
-- Test setup (error on no Kafka)
checkForKafka :: IO (Bool)
checkForKafka = do
  kConf <- newKafkaConf 
  kafka <- newKafka KafkaConsumer kConf
  addBrokers kafka brokerAddress
  me <- getAllMetadata kafka 1000
  return $ case me of 
    (Left _) -> False
    (Right _) -> True

main :: IO () 
main = do 
  hasKafka <- checkForKafka 
  if hasKafka then testmain
  else error "\n\n\
    \*******************************************************************************\n\
    \*Haskakafka's tests require an operable Kafka broker running on localhost:9092*\n\
    \*please follow the guide in Readme.md to set this up                          *\n\
    \*******************************************************************************\n"
