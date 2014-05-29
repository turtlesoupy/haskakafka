{-# LANGUAGE DeriveDataTypeable #-}

module Haskakafka 
(  Kafka
 , KafkaOffset(..)
 , KafkaType(..)
 , KafkaMessage(..)
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
 , produceMessage
 , pollEvents
 , drainOutQueue
) where

import Foreign
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Storable
import Foreign.C.String
import Foreign.C.Error
import Haskakafka.Internal
import System.IO
import Control.Monad
import Control.Exception
import Data.Typeable
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI

data KafkaError = 
    KafkaError String
  | KafkaInvalidReturnValue
  | KafkaBadSpecification String
  | KafkaTimedOut
  | KafkaResponseError RdKafkaRespErrT
  | KafkaUnknownTopicPartition
    deriving (Show, Typeable)


data KafkaMessage = KafkaMessage
    { messagePartition :: Int
    , messageOffset :: Int64
    , messagePayload :: BS.ByteString
    , messageKey :: Maybe BS.ByteString
    }
    deriving (Eq, Show, Read, Typeable)

data KafkaOffset = KafkaOffsetBeginning
                 | KafkaOffsetEnd
                 | KafkaOffsetStored
                 | KafkaOffset Int64

instance Exception KafkaError

data KafkaType = KafkaConsumer | KafkaProducer
data Kafka = Kafka { kafkaPtr :: RdKafkaTPtr}
data KafkaTopic = KafkaTopic { kafkaTopicPtr :: RdKafkaTopicTPtr }
data KafkaConf = KafkaConf {kafkaConfPtr :: RdKafkaConfTPtr}
data KafkaTopicConf = KafkaTopicConf {kafkaTopicConfPtr :: RdKafkaTopicConfTPtr}

kafkaTypeToRdKafkaType :: KafkaType -> RdKafkaTypeT
kafkaTypeToRdKafkaType KafkaConsumer = RdKafkaConsumer
kafkaTypeToRdKafkaType KafkaProducer = RdKafkaProducer

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
    et <- newRdKafkaT (kafkaTypeToRdKafkaType kafkaType) confPtr 
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

startConsuming :: KafkaTopic -> Int -> KafkaOffset -> IO ()
startConsuming (KafkaTopic topicPtr) partition offset = 
    let trueOffset = case offset of
                        KafkaOffsetBeginning -> -2
                        KafkaOffsetEnd -> -1
                        KafkaOffsetStored -> -1000
                        KafkaOffset i -> i
    in throwOnError $ rdKafkaConsumeStart topicPtr partition trueOffset


word8PtrToBS :: Int -> Word8Ptr -> IO (BS.ByteString)
word8PtrToBS len ptr = BSI.create len $ \bsptr -> 
    BSI.memcpy bsptr ptr len
    

consumeMessage :: KafkaTopic -> Int -> Int -> IO (Either KafkaError KafkaMessage)
consumeMessage (KafkaTopic topicPtr) partition timeout = do
    ptr <- rdKafkaConsume topicPtr (fromIntegral partition) (fromIntegral timeout)
    withForeignPtr ptr $ \realPtr ->
        if realPtr == nullPtr then getErrno >>= return . Left . kafkaRespErr 
        else do
            addForeignPtrFinalizer rdKafkaMessageDestroy ptr
            s <- peek realPtr
            if (err'RdKafkaMessageT s) /= RdKafkaRespErrNoError then return $ Left $ KafkaError $ rdKafkaErr2str $ err'RdKafkaMessageT s
            else do
                payload <- word8PtrToBS (len'RdKafkaMessageT s) (payload'RdKafkaMessageT s) 

                key <- if (key'RdKafkaMessageT s) == nullPtr then return Nothing 
                       else word8PtrToBS (keyLen'RdKafkaMessageT s) (key'RdKafkaMessageT s) >>= return . Just

                return $ Right $ KafkaMessage
                    (partition'RdKafkaMessageT s) 
                    (offset'RdKafkaMessageT s)
                    payload
                    key 

produceMessage :: KafkaTopic -> KafkaMessage -> IO (Maybe KafkaError)
produceMessage (KafkaTopic topicPtr) km = do
    let msgFlags = rdKafkaMsgFlagCopy
        (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr (messagePayload km)

    withForeignPtr payloadFPtr $ \payloadPtr -> do
        let passedPayload = payloadPtr `plusPtr` payloadOffset
        
        err <- rdKafkaProduce topicPtr (fromIntegral $ messagePartition km)
                msgFlags passedPayload (fromIntegral payloadLength)
                nullPtr (fromIntegral 0) nullPtr

        case err of
            -1 -> getErrno >>= return . Just . kafkaRespErr 
            0 -> return Nothing
            _ -> return $ Just $ KafkaInvalidReturnValue

pollEvents :: Kafka -> Int -> IO ()
pollEvents (Kafka kPtr) timeout = rdKafkaPoll kPtr timeout >> return ()

outboundQueueLength :: Kafka -> IO (Int)
outboundQueueLength (Kafka kPtr) = rdKafkaOutqLen kPtr

drainOutQueue :: Kafka -> IO ()
drainOutQueue k = do
    pollEvents k 100
    l <- outboundQueueLength k
    if l == 0 then return ()
    else drainOutQueue k

kafkaRespErr :: Errno -> KafkaError
kafkaRespErr (Errno num) = KafkaResponseError $ rdKafkaErrno2err (fromIntegral num)

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
listToTuple _ = error "list to tuple can only be called on even length lists"
