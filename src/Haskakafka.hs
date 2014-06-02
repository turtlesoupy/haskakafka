{-# LANGUAGE DeriveDataTypeable #-}

module Haskakafka 
(  Kafka
 , KafkaConf
 , KafkaMessage(..)
 , KafkaOffset(..)
 , KafkaMetadata(..)
 , KafkaBrokerMetadata(..)
 , KafkaTopicMetadata(..)
 , KafkaPartitionMetadata(..)
 , KafkaProduceMessage(..)
 , KafkaProducePartition(..)
 , KafkaTopic
 , KafkaTopicConf
 , KafkaType(..)
 , KafkaError (..)
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
 , produceMessageBatch
 , pollEvents
 , drainOutQueue
 , getAllMetadata
 , getTopicMetadata
 , module Haskakafka.InternalEnum
) where

import Control.Exception
import Control.Monad
import Data.Map.Strict (Map)
import Data.Typeable
import Foreign
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import Haskakafka.Internal
import Haskakafka.InternalEnum
import System.IO
import qualified Data.Map.Strict as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI

data KafkaError = 
    KafkaError String
  | KafkaInvalidReturnValue
  | KafkaBadSpecification String
  | KafkaResponseError RdKafkaRespErrT
    deriving (Eq, Show, Typeable)

instance Exception KafkaError

data KafkaMessage = KafkaMessage
    { messagePartition :: !Int
    , messageOffset :: !Int64
    , messagePayload :: !BS.ByteString
    , messageKey :: Maybe BS.ByteString
    }
    deriving (Eq, Show, Read, Typeable)

data KafkaProduceMessage = 
    KafkaProduceMessage 
      {-# UNPACK #-} !BS.ByteString
  | KafkaProduceKeyedMessage 
      {-# UNPACK #-} !BS.ByteString -- | Key
      {-# UNPACK #-} !BS.ByteString -- | Payload
  deriving (Eq, Show, Typeable)
      

data KafkaProducePartition = 
    KafkaSpecifiedPartition {-# UNPACK #-} !Int
  | KafkaUnassignedPartition

data KafkaOffset = KafkaOffsetBeginning
                 | KafkaOffsetEnd
                 | KafkaOffsetStored
                 | KafkaOffset Int64

data KafkaType = KafkaConsumer | KafkaProducer
data Kafka = Kafka { kafkaPtr :: RdKafkaTPtr}
data KafkaTopic = KafkaTopic 
  { kafkaTopicPtr :: RdKafkaTopicTPtr 
  , owningKafka :: Kafka -- prevents garbage collection 
  } 

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
newKafkaTopic k@(Kafka kafkaPtr) topicName (KafkaTopicConf confPtr) = do
    et <- newRdKafkaTopicT kafkaPtr topicName confPtr
    case et of 
        Left e -> throw $ KafkaError e
        Right x -> return $ KafkaTopic x k

addBrokers :: Kafka -> String -> IO ()
addBrokers (Kafka kptr) brokerStr = do
    numBrokers <- rdKafkaBrokersAdd kptr brokerStr
    when (numBrokers == 0) 
        (throw $ KafkaBadSpecification "No valid brokers specified")

startConsuming :: KafkaTopic -> Int -> KafkaOffset -> IO ()
startConsuming (KafkaTopic topicPtr _) partition offset = 
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
consumeMessage (KafkaTopic topicPtr _) partition timeout = do
    ptr <- rdKafkaConsume topicPtr (fromIntegral partition) (fromIntegral timeout)
    withForeignPtr ptr $ \realPtr ->
        if realPtr == nullPtr then getErrno >>= return . Left . kafkaRespErr 
        else do
            addForeignPtrFinalizer rdKafkaMessageDestroy ptr
            s <- peek realPtr
            if (err'RdKafkaMessageT s) /= RdKafkaRespErrNoError then return $ Left $ KafkaResponseError $ err'RdKafkaMessageT s
            else do
                payload <- word8PtrToBS (len'RdKafkaMessageT s) (payload'RdKafkaMessageT s) 

                key <- if (key'RdKafkaMessageT s) == nullPtr then return Nothing 
                       else word8PtrToBS (keyLen'RdKafkaMessageT s) (key'RdKafkaMessageT s) >>= return . Just

                return $ Right $ KafkaMessage
                    (partition'RdKafkaMessageT s) 
                    (offset'RdKafkaMessageT s)
                    payload
                    key 

produceMessage :: KafkaTopic -> KafkaProducePartition -> KafkaProduceMessage -> IO (Maybe KafkaError)
produceMessage (KafkaTopic topicPtr _) partition (KafkaProduceMessage payload) = do
    let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr payload

    withForeignPtr payloadFPtr $ \payloadPtr -> do
        let passedPayload = payloadPtr `plusPtr` payloadOffset
        
        handleProduceErr =<< 
          rdKafkaProduce topicPtr (producePartitionInteger partition)
            copyMsgFlags passedPayload (fromIntegral payloadLength)
            nullPtr (CSize 0) nullPtr

produceMessage (KafkaTopic topicPtr _) partition (KafkaProduceKeyedMessage key payload) = do
    let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr payload
        (keyFPtr, keyOffset, keyLength) = BSI.toForeignPtr key

    withForeignPtr payloadFPtr $ \payloadPtr -> do
        withForeignPtr keyFPtr $ \keyPtr -> do
          let passedPayload = payloadPtr `plusPtr` payloadOffset
              passedKey = keyPtr `plusPtr` keyOffset

          handleProduceErr =<< 
            rdKafkaProduce topicPtr (producePartitionInteger partition)
              copyMsgFlags passedPayload (fromIntegral payloadLength)
              passedKey (fromIntegral keyLength) nullPtr

produceMessageBatch :: KafkaTopic -> KafkaProducePartition -> [KafkaProduceMessage] -> IO ([(KafkaProduceMessage, KafkaError)])
produceMessageBatch (KafkaTopic topicPtr _) partition pms = do
  storables <- forM pms produceMessageToMessage
  withArray storables $ \batchPtr -> do
    batchPtrF <- newForeignPtr_ batchPtr
    numRet <- rdKafkaProduceBatch topicPtr partitionInt copyMsgFlags batchPtrF (length storables)
    if numRet == (length storables) then return []
    else do 
      errs <- mapM (\i -> return . err'RdKafkaMessageT =<< peekElemOff batchPtr i) 
                   [0..((fromIntegral $ length storables) - 1)]
      return [(m, KafkaResponseError e) | (m, e) <- (zip pms errs), e /= RdKafkaRespErrNoError]
  where
    partitionInt = (producePartitionInteger partition)
    produceMessageToMessage (KafkaProduceMessage bs) =  do
        let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr bs
        withForeignPtr payloadFPtr $ \payloadPtr -> do
          let passedPayload = payloadPtr `plusPtr` payloadOffset
          return $ RdKafkaMessageT 
              { err'RdKafkaMessageT = RdKafkaRespErrNoError 
              , partition'RdKafkaMessageT = fromIntegral partitionInt
              , len'RdKafkaMessageT = payloadLength
              , payload'RdKafkaMessageT = passedPayload
              , offset'RdKafkaMessageT = 0
              , keyLen'RdKafkaMessageT = 0
              , key'RdKafkaMessageT = nullPtr
              }
    produceMessageToMessage (KafkaProduceKeyedMessage bs kbs) =  do
        let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr bs
            (keyFPtr, keyOffset, keyLength) = BSI.toForeignPtr kbs
             
        withForeignPtr payloadFPtr $ \payloadPtr -> do
          withForeignPtr keyFPtr $ \keyPtr -> do
            let passedPayload = payloadPtr `plusPtr` payloadOffset
                passedKey = keyPtr `plusPtr` keyOffset

            return $ RdKafkaMessageT 
                { err'RdKafkaMessageT = RdKafkaRespErrNoError 
                , partition'RdKafkaMessageT = fromIntegral partitionInt
                , len'RdKafkaMessageT = payloadLength
                , payload'RdKafkaMessageT = passedPayload
                , offset'RdKafkaMessageT = 0
                , keyLen'RdKafkaMessageT = keyLength
                , key'RdKafkaMessageT = keyPtr
                }
      
      

{-# INLINE copyMsgFlags  #-}
copyMsgFlags :: Int
copyMsgFlags = rdKafkaMsgFlagCopy

{-# INLINE producePartitionInteger #-}
producePartitionInteger :: KafkaProducePartition -> CInt
producePartitionInteger KafkaUnassignedPartition = -1
producePartitionInteger (KafkaSpecifiedPartition n) = fromIntegral n

{-# INLINE handleProduceErr #-}
handleProduceErr :: Int -> IO (Maybe KafkaError)
handleProduceErr (- 1) = getErrno >>= return . Just . kafkaRespErr 
handleProduceErr 0 = return $ Nothing
handleProduceErr _ = return $ Just $ KafkaInvalidReturnValue


data KafkaMetadata = KafkaMetadata
    { brokers :: [KafkaBrokerMetadata]
    , topics :: [Either KafkaError KafkaTopicMetadata]
    } deriving (Eq, Show, Typeable)

data KafkaBrokerMetadata = KafkaBrokerMetadata
    { brokerId :: Int
    , brokerHost :: String
    , brokerPort :: Int
    } deriving (Eq, Show, Typeable)

data KafkaTopicMetadata = KafkaTopicMetadata
    { topicName :: String
    , topicPartitions :: [Either KafkaError KafkaPartitionMetadata]
    } deriving (Eq, Show, Typeable)

data KafkaPartitionMetadata = KafkaPartitionMetadata
    { partitionId :: Int
    , partitionLeader :: Int
    , partitionReplicas :: [Int] 
    , partitionIsrs :: [Int]
    } deriving (Eq, Show, Typeable)

getAllMetadata :: Kafka -> Int -> IO (Either KafkaError KafkaMetadata)
getAllMetadata k timeout = getMetadata k Nothing timeout

getTopicMetadata :: Kafka -> KafkaTopic -> Int -> IO (Either KafkaError KafkaTopicMetadata)
getTopicMetadata k kt timeout = do
  err <- getMetadata k (Just kt) timeout
  case err of 
    Left e -> return $ Left $ e
    Right md -> case (topics md) of 
      [(Left e)] -> return $ Left e
      [(Right tmd)] -> return $ Right tmd
      _ -> return $ Left $ KafkaError "Incorrect number of topics returned"

getMetadata :: Kafka -> Maybe KafkaTopic -> Int -> IO (Either KafkaError KafkaMetadata)
getMetadata (Kafka kPtr) mTopic timeout = alloca $ \mdDblPtr -> do
    err <- case mTopic of  
      Just (KafkaTopic kTopicPtr _) -> 
        rdKafkaMetadata kPtr False kTopicPtr mdDblPtr timeout
      Nothing -> do
        nullTopic <- newForeignPtr_ nullPtr
        rdKafkaMetadata kPtr True nullTopic mdDblPtr timeout

    case err of 
      RdKafkaRespErrNoError -> do
        mdPtr <- peek mdDblPtr
        md <- peek mdPtr
        retMd <- constructMetadata md
        rdKafkaMetadataDestroy mdPtr
        return $ Right $ retMd
      e -> return $ Left $ KafkaResponseError e

    where 
      constructMetadata md =  do
        let nBrokers = (brokerCnt'RdKafkaMetadataT md)
            brokersPtr = (brokers'RdKafkaMetadataT md)
            nTopics = (topicCnt'RdKafkaMetadataT md)
            topicsPtr = (topics'RdKafkaMetadataT md)

        brokerMds <- mapM (\i -> constructBrokerMetadata =<< peekElemOff brokersPtr i) [0..((fromIntegral nBrokers) - 1)]
        topicMds <- mapM (\i -> constructTopicMetadata =<< peekElemOff topicsPtr i) [0..((fromIntegral nTopics) - 1)]
        return $ KafkaMetadata brokerMds topicMds

      constructBrokerMetadata bmd = do
        hostStr <- peekCString (host'RdKafkaMetadataBrokerT bmd)
        return $ KafkaBrokerMetadata 
                  (id'RdKafkaMetadataBrokerT bmd)
                  (hostStr)
                  (port'RdKafkaMetadataBrokerT bmd)

      constructTopicMetadata tmd = do
        case (err'RdKafkaMetadataTopicT tmd) of
          RdKafkaRespErrNoError -> do
            let nPartitions = (partitionCnt'RdKafkaMetadataTopicT tmd)
                partitionsPtr = (partitions'RdKafkaMetadataTopicT tmd)

            topicStr <- peekCString (topic'RdKafkaMetadataTopicT tmd)
            partitionsMds <- mapM (\i -> constructPartitionMetadata =<< peekElemOff partitionsPtr i) [0..((fromIntegral nPartitions) - 1)]
            return $ Right $ KafkaTopicMetadata topicStr partitionsMds
          e -> return $ Left $ KafkaResponseError e

      constructPartitionMetadata pmd = do
        case (err'RdKafkaMetadataPartitionT pmd) of
          RdKafkaRespErrNoError -> do
            let nReplicas = (replicaCnt'RdKafkaMetadataPartitionT pmd)
                replicasPtr = (replicas'RdKafkaMetadataPartitionT pmd)
                nIsrs = (isrCnt'RdKafkaMetadataPartitionT pmd)
                isrsPtr = (isrs'RdKafkaMetadataPartitionT pmd)
            replicas <- mapM (\i -> peekElemOff replicasPtr i) [0..((fromIntegral nReplicas) - 1)]
            isrs <- mapM (\i -> peekElemOff isrsPtr i) [0..((fromIntegral nIsrs) - 1)]
            return $ Right $ KafkaPartitionMetadata 
              (id'RdKafkaMetadataPartitionT pmd)              
              (leader'RdKafkaMetadataPartitionT pmd)
              (map fromIntegral replicas)
              (map fromIntegral isrs)
          e -> return $ Left $ KafkaResponseError e

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
stopConsuming (KafkaTopic topicPtr _) partition = 
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
