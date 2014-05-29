{-# LANGUAGE DeriveDataTypeable #-}

module Haskakafka 
(  Kafka
 , KafkaConf
 , KafkaTopicConf
 , hPrintKafkaProperties
 , hPrintKafka
 , newKafkaConf
 , dumpKafkaConf
 , newKafkaTopicConf
 , newKafka
 , newKafkaTopic
 , dumpKafkaTopicConf
 , addBrokers
 , startConsuming
 , consumeMessage
 , stopConsuming
 , module Haskakafka.InternalEnum
) where

import Foreign
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Storable
import Foreign.C.String
import Haskakafka.Internal
import Haskakafka.InternalEnum
import System.IO
import Control.Monad
import Control.Exception
import Data.Typeable
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

data KafkaError = 
    KafkaError String
  | KafkaTimedOut
  | KafkaUnknownTopicPartition
  | KafkaBadSpecification String
    deriving (Show, Typeable)

instance Exception KafkaError

type KafkaType = RdKafkaTypeT

data Kafka = Kafka { kafkaPtr :: RdKafkaTPtr}
data KafkaTopic = KafkaTopic { kafkaTopicPtr :: RdKafkaTopicTPtr }
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

newKafka :: KafkaType -> KafkaConf -> IO Kafka
newKafka kafkaType (KafkaConf confPtr) = do
    et <- newRdKafkaT kafkaType confPtr 
    case et of 
        Left e -> error e
        Right x -> return $ Kafka x

newKafkaTopic :: Kafka -> String -> KafkaTopicConf -> IO (KafkaTopic)
newKafkaTopic (Kafka kafkaPtr) topicName (KafkaTopicConf confPtr) = do
    et <- newRdKafkaTopicT kafkaPtr topicName confPtr
    case et of 
        Left e -> throw $ KafkaError e
        Right x -> return $ KafkaTopic x

addBrokers :: Kafka -> String -> IO ()
addBrokers (Kafka kptr) brokerStr = do
    numBrokers <- rdKafkaBrokersAdd kptr brokerStr
    when (numBrokers == 0) 
        (throw $ KafkaBadSpecification "No valid brokers specified")

startConsuming :: KafkaTopic -> Int -> Int64 -> IO ()
startConsuming (KafkaTopic topicPtr) partition offset = 
    throwOnError $ rdKafkaConsumeStart topicPtr partition offset

consumeMessage :: KafkaTopic -> Int -> Int -> IO (Either KafkaError String)
consumeMessage (KafkaTopic topicPtr) partition timeout = do
    ptr <- rdKafkaConsume topicPtr (fromIntegral partition) (fromIntegral timeout)
    withForeignPtr ptr $ \realPtr ->
        if realPtr == nullPtr then kafkaErrnoString >>= return . Left . KafkaError
        else do
            addForeignPtrFinalizer rdKafkaMessageDestroy ptr
            s <- peek realPtr
            print s
            return $ Right "ABC"

stopConsuming :: KafkaTopic -> Int -> IO ()
stopConsuming (KafkaTopic topicPtr) partition = 
    throwOnError $ rdKafkaConsumeStop topicPtr partition

throwOnError :: IO (Maybe String) -> IO ()
throwOnError action = do
    m <- action
    case m of 
        Just e -> throw $ KafkaError e
        Nothing -> return ()


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
