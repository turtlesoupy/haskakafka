module Haskakafka 
(  Kafka
 , KafkaConf
 , KafkaTopicConf
 , hPrintKafkaProperties
 , hPrintKafka
 , newKafkaConf
 , dumpKafkaConf
 , newKafkaTopicConf
 , dumpKafkaTopicConf
) where

import Foreign
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Storable
import Foreign.C.String
import Haskakafka.Internal
import System.IO
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

data Kafka = Kafka { kafkaPtr :: RdKafkaTPtr}
data KafkaConf = KafkaConf {kafkaConfPtr :: RdKafkaConfTPtr}
data KafkaTopicConf = KafkaTopicConf {kafkaTopicConfPtr :: RdKafkaTopicConfTPtr}

hPrintKafkaProperties :: Handle -> IO ()
hPrintKafkaProperties h = handleToCFile h "w" >>= rdKafkaConfPropertiesShow

hPrintKafka :: Handle -> Kafka -> IO ()
hPrintKafka h k = handleToCFile h "w" >>= \f -> rdKafkaDump f (kafkaPtr k)

newKafkaTopicConf :: IO KafkaTopicConf
newKafkaTopicConf = newRdKafkaTopicConfT >>= return . KafkaTopicConf

newKafkaConf :: IO KafkaConf
newKafkaConf = newRdKafkaConfT >>= return . KafkaConf

dumpKafkaTopicConf :: KafkaTopicConf -> IO (Map String String)
dumpKafkaTopicConf (KafkaTopicConf kptr) = 
    parseDump (\sizeptr -> rdKafkaTopicConfDump kptr sizeptr)

dumpKafkaConf :: KafkaConf -> IO (Map String String)
dumpKafkaConf (KafkaConf kptr) = do
    parseDump (\sizeptr -> rdKafkaConfDump kptr sizeptr)

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map String String)
parseDump cstr = alloca $ \sizeptr -> do
    strPtr <- cstr sizeptr 
    size <- peek sizeptr

    keysAndValues <- mapM (\i -> peekCString =<< peekElemOff strPtr i) [0..((fromIntegral size) - 1)]

    let ret = Map.fromList $ listToTuple keysAndValues
    rdKafkaConfDumpFree strPtr size
    return ret

listToTuple :: [String] -> [(String, String)]
listToTuple [] = []
listToTuple (k:v:t) = (k, v) : listToTuple t
