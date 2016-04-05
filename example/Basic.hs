{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RankNTypes #-}
import Haskakafka

import qualified Data.ByteString.Char8 as C8
import           Data.Maybe (fromMaybe)
import           Control.Monad (forM_)

import           System.Console.CmdArgs
import           Text.Show.Pretty (ppShow)


data BasicMode = Consumer | Producer | List | All
  deriving (Data, Typeable, Show, Eq)

data CArgs = CArgs
  { brokers'CArgs   :: Maybe String
  , topic'CArgs     :: Maybe String
  , mode'CArgs      :: BasicMode
  , partition'CArgs :: Maybe Int
  , pretty'CArgs :: Bool
  } deriving (Data, Typeable, Show, Eq)

producerExample ::
                String -> String -> Int ->
                (forall a. Show a => a -> String) -> IO ()
producerExample brokerNames ourTopic partition showFn = do
  let
      -- Optionally, we can configure certain parameters for Kafka
      kafkaConfig = [("socket.timeout.ms", "50000")]
      topicConfig = [("request.timeout.ms", "50000")]

      -- Payloads are just ByteStrings
      samplePayload = C8.pack "Hello world"

  -- withKafkaProducer opens a producer connection and gives us
  -- two objects for subsequent use.
  withKafkaProducer kafkaConfig topicConfig
                    brokerNames ourTopic
                    $ \kafka topic -> do

    -- Produce a single unkeyed message to partition
    let message = KafkaProduceMessage samplePayload
    _ <- produceMessage topic (KafkaSpecifiedPartition partition) message

    -- Produce a single keyed message
    let keyMessage = KafkaProduceKeyedMessage (C8.pack "Key") samplePayload
    _ <- produceKeyedMessage topic keyMessage

    -- We can also use the batch API for better performance
    _ <- produceMessageBatch topic KafkaUnassignedPartition [message, keyMessage]

    putStrLn "Done producing messages, here was our config: "
    dumpConfFromKafka kafka >>= \d -> putStrLn $ "Kafka config: " ++ (showFn d)
    dumpConfFromKafkaTopic topic >>= \d -> putStrLn $ "Topic config: " ++ (showFn d)

consumerExample ::
                String -> String -> Int ->
                (forall a. Show a => a -> String) -> IO ()
consumerExample brokerNames ourTopic partition showFn = do
  let
      -- Optionally, we can configure certain parameters for Kafka
      kafkaConfig = [("socket.timeout.ms", "50000")]
      topicConfig = [("request.timeout.ms", "50000")]

  -- withKafkaConsumer opens a consumer connection and starts consuming
  withKafkaConsumer kafkaConfig topicConfig
                    brokerNames ourTopic
                    partition -- locked to a specific partition for each consumer
                    KafkaOffsetBeginning -- start reading from beginning
                    -- (alternatively, use
                    -- KafkaOffsetEnd, KafkaOffset or KafkaOffsetStored)
                    $ \kafka topic -> do

    -- Consume a single message at a time
    let timeoutMs = 3000
    me <- consumeMessage topic partition timeoutMs
    case me of
      (Left err) -> putStrLn $ "Uh oh, an error! " ++ (showFn err)
      (Right m) -> putStrLn $ "Woo, payload was " ++ (C8.unpack $ messagePayload m)

    -- For better performance, consume in batches
    let maxMessages = 10
    mes <- consumeMessageBatch topic partition timeoutMs maxMessages
    case mes of
      (Left err) -> putStrLn $
        "Something went wrong in batch consume! " ++ (showFn err)
      (Right ms) -> forM_ ms $ \msg -> do
        putStrLn $ "Woohoo, we got: " ++ (showFn msg)

    -- Be a little less noisy
    setLogLevel kafka KafkaLogCrit

metadataExample :: String -> (forall a. Show a => a -> String) -> IO ()
metadataExample brokerNames showFn = do
  -- we can also fetch metadata about our Kafka infrastructure
  let timeoutMs = 1000
  emd <- fetchBrokerMetadata [] brokerNames timeoutMs
  case emd of
    (Left err) -> putStrLn $ "Uh oh, error time: " ++ (showFn err)
    (Right md) -> putStrLn $ "Kafka metadata: " ++ (showFn md)

runExample ::
           BasicMode -> String -> String -> Int ->
           (forall a. (Show a) => a -> String) -> IO ()
runExample Consumer b t p pp = consumerExample b t p pp
runExample Producer b t p pp = producerExample b t p pp
runExample List     b _ _ pp = metadataExample b pp
runExample All      b t p pp = do
  consumerExample b t p pp
  producerExample b t p pp
  metadataExample b pp

parseExample :: CArgs -> IO ()
parseExample (CArgs b t m p pp) = runExample
  m
  (fromMaybe "localhost:9092" b)
  (fromMaybe "test_topic" t)
  (fromMaybe 0 p)
  (if pp then ppShow else show)

cargs :: CArgs
cargs = CArgs
  { brokers'CArgs = def
    &= typ "<brokers>"
    &= help "Comma separated list in format <hostname>:<port>,<hostname>:<port>"
    &= explicit
    &= name "brokers"
    &= name "b"
  , topic'CArgs = def
    &= typ "<topic>"
    &= help "Topic to fetch / produce"
    &= explicit
    &= name "topic"
    &= name "t"
  , mode'CArgs = enum
    [ Consumer &= help "Consumer mode" &= name "C"
    , Producer &= help "Producer mode" &= name "P"
    , List     &= help "Metadata list mode" &= name "L"
    , All      &= help "Run producer, consumer, and metadata list" &= name "A"
    ]
  , partition'CArgs = def
    &= typ "<num>"
    &= help "Partition (-1 for random partitioner when using producer)"
    &= explicit
    &= name "p"
  , pretty'CArgs = def
    &= help "Pretty print output"
    &= explicit
    &= name "pretty"
  } &= help "Fetch metadata, produce, and consume a message"
    &= program "basic example"

main :: IO ()
main = parseExample =<< cmdArgs cargs
