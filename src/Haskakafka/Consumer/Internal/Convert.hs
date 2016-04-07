module Haskakafka.Consumer.Internal.Convert

where

import           Control.Monad
import           Foreign
import           Foreign.C.String
import           Haskakafka.Consumer.Internal.Types
import           Haskakafka.InternalRdKafka
import           Haskakafka.InternalShared

-- | Converts offsets sync policy to integer (the way Kafka understands it):
--
--     * @OffsetSyncDisable == -1@
--
--     * @OffsetSyncImmediate == 0@
--
--     * @OffsetSyncInterval ms == ms@
offsetSyncToInt :: OffsetStoreSync -> Int
offsetSyncToInt sync =
    case sync of
        OffsetSyncDisable -> -1
        OffsetSyncImmediate -> 0
        OffsetSyncInterval ms -> ms
{-# INLINE offsetSyncToInt #-}

fromNativeTopicPartitionList :: RdKafkaTopicPartitionListT -> IO [KafkaTopicPartition]
fromNativeTopicPartitionList pl =
    let count = cnt'RdKafkaTopicPartitionListT pl
        elems = elems'RdKafkaTopicPartitionListT pl
    in mapM (peekElemOff elems >=> toPart) [0..(fromIntegral count - 1)]
    where
        toPart :: RdKafkaTopicPartitionT -> IO KafkaTopicPartition
        toPart p = do
            topic <- peekCString $ topic'RdKafkaTopicPartitionT p
            return KafkaTopicPartition {
                ktpTopicName = TopicName topic,
                ktpPartition = partition'RdKafkaTopicPartitionT p,
                ktpOffset    = int64ToOffset $ offset'RdKafkaTopicPartitionT p
            }

toNativeTopicPartitionList :: [KafkaTopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
    pl <- newRdKafkaTopicPartitionListT (length ps)
    mapM_ (\p -> do
        let TopicName tn = ktpTopicName p
            tp = ktpPartition p
            to = offsetToInt64 $ ktpOffset p
        _ <- rdKafkaTopicPartitionListAdd pl tn tp
        rdKafkaTopicPartitionListSetOffset pl tn tp to) ps
    return pl

