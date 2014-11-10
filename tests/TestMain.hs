module Main (main) where

import Haskakafka
import Test.Hspec
import Text.Regex.Posix

brokerAddress :: String
brokerAddress = "localhost:9092"

testmain :: IO ()
testmain = hspec $ do
  describe "RdKafka versioning" $ do
    it "should be a valid version number" $ do
      rdKafkaVersionStr `shouldSatisfy` (=~"[0-9]+(.[0-9]+)+")

  describe "Supported properties" $ do
    it "should list supported properties" $ do
      props <- supportedKafkaConfProperties
      props `shouldSatisfy` (\x -> (length x) > 0)

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

--doConsume :: IO ()
--doConsume = do
--    kConf <- newKafkaConf
--    kafka <- newKafka KafkaConsumer kConf
--    addBrokers kafka "localhost:9092"
--    kTopicConf <- newKafkaTopicConf
--    topic <- newKafkaTopic kafka "test" kTopicConf
--
--    startConsuming topic 0 (KafkaOffsetBeginning)
--    _ <- forever $ do
--          m <- consumeMessage topic 0 (1000 * 1000)
--          print m
--    stopConsuming topic 0
--
--doProduce :: IO ()
--doProduce = do
--    kConf <- newKafkaConf
--    kafka <- newKafka KafkaProducer kConf
--    addBrokers kafka "localhost:9092"
--    kTopicConf <- newKafkaTopicConf
--    topic <- newKafkaTopic kafka "test" kTopicConf
--    let me = KafkaMessage 0 0 (BS.pack "hi") Nothing
--    err <- produceMessage topic me
--
--    drainOutQueue kafka
--            
--    print err
--
--
