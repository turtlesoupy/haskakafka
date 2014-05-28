module Haskakafka where

import Haskakafka.Internal
import System.IO

type Kafka = RdKafkaTPtr

printKafkaProperties :: IO ()
printKafkaProperties = c_stdout >>= rdKafkaConfPropertiesShow

printKafka :: Kafka -> IO ()
printKafka k = do
    s <- c_stdout 
    rdKafkaDump s k
