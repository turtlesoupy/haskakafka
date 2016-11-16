module Haskakafka
( fetchBrokerMetadata
, withKafkaConsumer
, consumeMessage
, consumeMessageBatch
, withKafkaProducer
, produceMessage
, produceKeyedMessage
, produceMessageBatch
, storeOffset
, seekToOffset
, getAllMetadata
, getTopicMetadata
, handleProduceErr
, producePartitionInteger
, pollEvents
, pollEventsSafe
, queryWatermarkOffsets

-- Internal objects
, IS.newKafka
, IS.newKafkaTopic
, IS.dumpConfFromKafka
, IS.dumpConfFromKafkaTopic
, IS.setLogLevel
, IS.hPrintSupportedKafkaConf
, IS.hPrintKafka
, rdKafkaVersionStr

-- Type re-exports
, IT.Kafka(..)
, IT.KafkaTopic(..)

, IT.KafkaOffset(..)
, IT.KafkaMessage(..)

, IT.KafkaProduceMessage(..)
, IT.KafkaProducePartition(..)

, IT.KafkaMetadata(..)
, IT.KafkaBrokerMetadata(..)
, IT.KafkaTopicMetadata(..)
, IT.KafkaPartitionMetadata(..)

, IT.KafkaLogLevel(..)
, IT.KafkaError(..)
, RDE.RdKafkaRespErrT(..)

-- Pseudo-internal
, addBrokers
, startConsuming
, stopConsuming
, drainOutQueue

) where

import           Haskakafka.InternalRdKafka
import           Haskakafka.InternalRdKafkaEnum
import           Haskakafka.InternalSetup
import           Haskakafka.InternalShared
import           Haskakafka.InternalTypes

import           Control.Exception
import           Control.Monad
import           Foreign
import           Foreign.C.Error
import           Foreign.C.String
import           Foreign.C.Types

import qualified Data.ByteString.Internal       as BSI
import qualified Haskakafka.InternalRdKafkaEnum as RDE
import qualified Haskakafka.InternalSetup       as IS
import qualified Haskakafka.InternalTypes       as IT

import Data.Either

-- | Adds a broker string to a given kafka instance. You
-- probably shouldn't use this directly (see 'withKafkaConsumer'
-- and 'withKafkaProducer')
addBrokers :: Kafka -> String -> IO ()
addBrokers (Kafka kptr _) brokerStr = do
    numBrokers <- rdKafkaBrokersAdd kptr brokerStr
    when (numBrokers == 0)
        (throw $ KafkaBadSpecification "No valid brokers specified")

-- | Starts consuming for a given topic. You probably do not need
-- to call this directly (it is called automatically by 'withKafkaConsumer') but
-- 'consumeMessage' won't work without it. This function is non-blocking.
startConsuming :: KafkaTopic -> Int -> KafkaOffset -> IO ()
startConsuming (KafkaTopic topicPtr _ _) partition offset =
    throwOnError $ rdKafkaConsumeStart topicPtr partition $ offsetToInt64 offset

-- | Stops consuming for a given topic. You probably do not need to call
-- this directly (it is called automatically when 'withKafkaConsumer' terminates).
stopConsuming :: KafkaTopic -> Int -> IO ()
stopConsuming (KafkaTopic topicPtr _ _) partition =
    throwOnError $ rdKafkaConsumeStop topicPtr partition

-- | Consumes a single message from a Kafka topic, waiting up to a given timeout
consumeMessage :: KafkaTopic
               -> Int -- ^ partition number to consume from (must match 'withKafkaConsumer')
               -> Int -- ^ the timeout, in milliseconds (10^3 per second)
               -> IO (Either KafkaError KafkaMessage) -- ^ Left on error or timeout, right for success
consumeMessage (KafkaTopic topicPtr _ _) partition timeout = do
  ptr <- rdKafkaConsume topicPtr (fromIntegral partition) (fromIntegral timeout)
  fromMessagePtr ptr

-- | Consumes a batch of messages from a Kafka topic, waiting up to a given timeout. Partial results
-- will be returned if a timeout occurs.
consumeMessageBatch :: KafkaTopic
                    -> Int -- ^ partition number to consume from (must match 'withKafkaConsumer')
                    -> Int -- ^ timeout in milliseconds (10^3 per second)
                    -> Int -- ^ maximum number of messages to return
                    -> IO (Either KafkaError [KafkaMessage]) -- ^ Left on error, right with up to 'maxMessages' messages on success
consumeMessageBatch (KafkaTopic topicPtr _ _) partition timeout maxMessages =
  allocaArray maxMessages $ \outputPtr -> do
    numMessages <- rdKafkaConsumeBatch topicPtr (fromIntegral partition) timeout outputPtr (fromIntegral maxMessages)
    if numMessages < 0 then getErrno >>= return . Left . kafkaRespErr
    else do
      ms <- if numMessages /= 0 then
              forM [0..(numMessages - 1)] $ \mnum -> do
              storablePtr <- peekElemOff outputPtr (fromIntegral mnum)
              storable <- peek storablePtr
              ret <- fromMessageStorable storable
              fptr <- newForeignPtr_ storablePtr
              withForeignPtr fptr $ \realPtr ->
                rdKafkaMessageDestroy realPtr
              if (err'RdKafkaMessageT storable) /= RdKafkaRespErrNoError then
                  return $ Left $ KafkaResponseError $ err'RdKafkaMessageT storable
              else
                  return $ Right ret
            else return []
      case lefts ms of
        [] -> return $ Right $ rights ms
        l  -> return $ Left $ head l

seekToOffset :: KafkaTopic
    -> Int -- ^ partition number
    -> KafkaOffset -- ^ destination
    -> Int -- ^ timeout in milliseconds
    -> IO (Maybe KafkaError)
seekToOffset (KafkaTopic ptr _ _) p ofs timeout = do
  err <- rdKafkaSeek ptr (fromIntegral p)
      (fromIntegral $ offsetToInt64 ofs) timeout
  case err of
    RdKafkaRespErrNoError -> return Nothing
    e -> return $ Just $ KafkaResponseError e

-- | Store a partition's offset in librdkafka's offset store. This function only needs to be called
-- if auto.commit.enable is false. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
-- for information on how to configure the offset store.
storeOffset :: KafkaTopic -> Int -> Int -> IO (Maybe KafkaError)
storeOffset (KafkaTopic topicPtr _ _) partition offset = do
  err <- rdKafkaOffsetStore topicPtr (fromIntegral partition) (fromIntegral offset)
  case err of
    RdKafkaRespErrNoError -> return Nothing
    e -> return $ Just $ KafkaResponseError e

-- | Produce a single unkeyed message to either a random partition or specified partition. Since
-- librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'drainOutQueue' to wait for queue to empty.
produceMessage :: KafkaTopic -- ^ topic pointer
               -> KafkaProducePartition  -- ^ the partition to produce to. Specify 'KafkaUnassignedPartition' if you don't care.
               -> KafkaProduceMessage  -- ^ the message to enqueue. This function is undefined for keyed messages.
               -> IO (Maybe KafkaError) -- Nothing on success, error if something went wrong.
produceMessage (KafkaTopic topicPtr _ _) partition (KafkaProduceMessage payload) = do
    let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr payload

    withForeignPtr payloadFPtr $ \payloadPtr -> do
        let passedPayload = payloadPtr `plusPtr` payloadOffset

        handleProduceErr =<<
          rdKafkaProduce topicPtr (producePartitionInteger partition)
            copyMsgFlags passedPayload (fromIntegral payloadLength)
            nullPtr (CSize 0) nullPtr

produceMessage _ _ (KafkaProduceKeyedMessage _ _) = undefined

-- | Produce a single keyed message. Since librdkafka is backed by a queue, this function can return
-- before messages are sent. See 'drainOutQueue' to wait for a queue to be empty
produceKeyedMessage :: KafkaTopic -- ^ topic pointer
                    -> KafkaProduceMessage  -- ^ keyed message. This function is undefined for unkeyed messages.
                    -> IO (Maybe KafkaError) -- ^ Nothing on success, error if something went wrong.
produceKeyedMessage _ (KafkaProduceMessage _) = undefined
produceKeyedMessage (KafkaTopic topicPtr _ _) (KafkaProduceKeyedMessage key payload) = do
    let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr payload
        (keyFPtr, keyOffset, keyLength) = BSI.toForeignPtr key

    withForeignPtr payloadFPtr $ \payloadPtr -> do
        withForeignPtr keyFPtr $ \keyPtr -> do
          let passedPayload = payloadPtr `plusPtr` payloadOffset
              passedKey = keyPtr `plusPtr` keyOffset

          handleProduceErr =<<
            rdKafkaProduce topicPtr (producePartitionInteger KafkaUnassignedPartition)
              copyMsgFlags passedPayload (fromIntegral payloadLength)
              passedKey (fromIntegral keyLength) nullPtr

-- | Produce a batch of messages. Since librdkafka is backed by a queue, this function can return
-- before messages are sent. See 'drainOutQueue' to wait for the queue to be empty.
produceMessageBatch :: KafkaTopic  -- ^ topic pointer
                    -> KafkaProducePartition -- ^ partition to produce to. Specify 'KafkaUnassignedPartition' if you don't care, or you have keyed messsages.
                    -> [KafkaProduceMessage] -- ^ list of messages to enqueue.
                    -> IO ([(KafkaProduceMessage, KafkaError)]) -- list of failed messages with their errors. This will be empty on success.
produceMessageBatch (KafkaTopic topicPtr _ _) partition pms = do
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
        withForeignPtr topicPtr $ \ptrTopic -> do
            withForeignPtr payloadFPtr $ \payloadPtr -> do
              let passedPayload = payloadPtr `plusPtr` payloadOffset
              return $ RdKafkaMessageT
                  { err'RdKafkaMessageT = RdKafkaRespErrNoError
                  , topic'RdKafkaMessageT = ptrTopic
                  , partition'RdKafkaMessageT = fromIntegral partitionInt
                  , len'RdKafkaMessageT = payloadLength
                  , payload'RdKafkaMessageT = passedPayload
                  , offset'RdKafkaMessageT = 0
                  , keyLen'RdKafkaMessageT = 0
                  , key'RdKafkaMessageT = nullPtr
                  , private'RdKafkaMessageT = nullPtr
                  }
    produceMessageToMessage (KafkaProduceKeyedMessage kbs bs) =  do
        let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr bs
            (keyFPtr, keyOffset, keyLength) = BSI.toForeignPtr kbs

        withForeignPtr topicPtr $ \ptrTopic ->
            withForeignPtr payloadFPtr $ \payloadPtr -> do
              withForeignPtr keyFPtr $ \keyPtr -> do
                let passedPayload = payloadPtr `plusPtr` payloadOffset
                    passedKey = keyPtr `plusPtr` keyOffset

                return $ RdKafkaMessageT
                    { err'RdKafkaMessageT = RdKafkaRespErrNoError
                    , topic'RdKafkaMessageT = ptrTopic
                    , partition'RdKafkaMessageT = fromIntegral partitionInt
                    , len'RdKafkaMessageT = payloadLength
                    , payload'RdKafkaMessageT = passedPayload
                    , offset'RdKafkaMessageT = 0
                    , keyLen'RdKafkaMessageT = keyLength
                    , key'RdKafkaMessageT = passedKey
                    , private'RdKafkaMessageT = nullPtr
                    }

-- | Connects to Kafka broker in producer mode for a given topic, taking a function
-- that is fed with 'Kafka' and 'KafkaTopic' instances. After receiving handles you
-- should be using 'produceMessage', 'produceKeyedMessage' and 'produceMessageBatch'
-- to publish messages. This function drains the outbound queue automatically before returning.
withKafkaProducer :: ConfigOverrides -- ^ config overrides for kafka. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> ConfigOverrides -- ^ config overrides for topic. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> String -- ^ broker string, e.g. localhost:9092
                  -> String -- ^ topic name
                  -> (Kafka -> KafkaTopic -> IO a)  -- ^ your code, fed with 'Kafka' and 'KafkaTopic' instances for subsequent interaction.
                  -> IO a -- ^ returns what your code does
withKafkaProducer configOverrides topicConfigOverrides brokerString tName cb =
  bracket
    (do
      kafka <- newKafka RdKafkaProducer configOverrides
      addBrokers kafka brokerString
      topic <- newKafkaTopic kafka tName topicConfigOverrides
      return (kafka, topic)
    )
    (\(kafka, _) -> drainOutQueue kafka)
    (\(k, t) -> cb k t)

-- | Connects to Kafka broker in consumer mode for a specific partition,
-- taking a function that is fed with
-- 'Kafka' and 'KafkaTopic' instances. After receiving handles, you should be using
-- 'consumeMessage' and 'consumeMessageBatch' to receive messages. This function
-- automatically starts consuming before calling your code.
withKafkaConsumer :: ConfigOverrides -- ^ config overrides for kafka. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> ConfigOverrides -- ^ config overrides for topic. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> String -- ^ broker string, e.g. localhost:9092
                  -> String -- ^ topic name
                  -> Int -- ^ partition to consume from. Locked until the function returns.
                  -> KafkaOffset -- ^ where to begin consuming in the partition.
                  -> (Kafka -> KafkaTopic -> IO a)  -- ^ your cod, fed with 'Kafka' and 'KafkaTopic' instances for subsequent interaction.
                  -> IO a -- ^ returns what your code does
withKafkaConsumer configOverrides topicConfigOverrides brokerString tName partition offset cb =
  bracket
    (do
      kafka <- newKafka RdKafkaConsumer configOverrides
      addBrokers kafka brokerString
      topic <- newKafkaTopic kafka tName topicConfigOverrides
      startConsuming topic partition offset
      return (kafka, topic)
    )
    (\(_, topic) -> stopConsuming topic partition)
    (\(k, t) -> cb k t)

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

-- | Opens a connection with brokers and returns metadata about topics, partitions and brokers.
fetchBrokerMetadata :: ConfigOverrides -- ^ connection overrides, see <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
                    -> String  -- broker connection string, e.g. localhost:9092
                    -> Int -- timeout for the request, in milliseconds (10^3 per second)
                    -> IO (Either KafkaError KafkaMetadata) -- Left on error, Right with metadata on success
fetchBrokerMetadata configOverrides brokerString timeout = do
  kafka <- newKafka RdKafkaConsumer configOverrides
  addBrokers kafka brokerString
  getAllMetadata kafka timeout

-- | Grabs all metadata from a given Kafka instance.
getAllMetadata :: Kafka
               -> Int  -- ^ timeout in milliseconds (10^3 per second)
               -> IO (Either KafkaError KafkaMetadata)
getAllMetadata k timeout = getMetadata k Nothing timeout

-- | Grabs topic metadata from a given Kafka topic instance
getTopicMetadata :: Kafka
                 -> KafkaTopic
                 -> Int  -- ^ timeout in milliseconds (10^3 per second)
                 -> IO (Either KafkaError KafkaTopicMetadata)
getTopicMetadata k kt timeout = do
  err <- getMetadata k (Just kt) timeout
  case err of
    Left e -> return $ Left $ e
    Right md -> case (topics md) of
      [(Left e)] -> return $ Left e
      [(Right tmd)] -> return $ Right tmd
      _ -> return $ Left $ KafkaError "Incorrect number of topics returned"

getMetadata :: Kafka -> Maybe KafkaTopic -> Int -> IO (Either KafkaError KafkaMetadata)
getMetadata (Kafka kPtr _) mTopic timeout = alloca $ \mdDblPtr -> do
    err <- case mTopic of
      Just (KafkaTopic kTopicPtr _ _) ->
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

-- | Get the current watermarks for a given topic
queryWatermarkOffsets
   :: Kafka   -- ^
   -> String  -- ^ topic
   -> Int     -- ^ partition
   -> Int     -- ^ timeout in milliseconds (10^3 per second)
   -> IO (Either String (Int64,Int64))
queryWatermarkOffsets (Kafka kafkaPtr _) topic partition timeout = do
   let
      partition' = fromIntegral partition
      timeout' = fromIntegral timeout
   allocaBytesAligned 8 8 $ \lo ->
     allocaBytesAligned 8 8 $ \hi ->
        withCString topic $ \topic' -> do

         ret <- rdKafkaQueryWatermarkOffsetsInternal kafkaPtr topic' partition' lo hi timeout'
         if ret == RdKafkaRespErrNoError
         then do
             retLo <- peek lo
             retHi <- peek hi
             return $ Right (fromIntegral retLo,fromIntegral retHi)
         else
             return . Left $ rdKafkaErr2str ret

pollEvents :: Kafka -> Int -> IO ()
pollEvents (Kafka kPtr _) timeout = rdKafkaPoll kPtr timeout >> return ()

pollEventsSafe :: Kafka -> Int -> IO ()
pollEventsSafe (Kafka kPtr _) timeout = do
  _ <- withForeignPtr kPtr $ \realPtr -> do
    rdKafkaPollSafe realPtr timeout
  return ()

outboundQueueLength :: Kafka -> IO (Int)
outboundQueueLength (Kafka kPtr _) = rdKafkaOutqLen kPtr

-- | Drains the outbound queue for a producer. This function is called automatically at the end of
-- 'withKafkaProducer' and usually doesn't need to be called directly.
drainOutQueue :: Kafka -> IO ()
drainOutQueue k = do
    pollEvents k 100
    l <- outboundQueueLength k
    if l == 0 then return ()
    else drainOutQueue k
