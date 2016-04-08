module Haskakafka.InternalShared
where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Internal       as BSI
import           Foreign
import           Foreign.C.Error
import           Haskakafka.InternalRdKafka
import           Haskakafka.InternalRdKafkaEnum
import           Haskakafka.InternalTypes

word8PtrToBS :: Int -> Word8Ptr -> IO BS.ByteString
word8PtrToBS len ptr = BSI.create len $ \bsptr ->
    BSI.memcpy bsptr ptr len

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError KafkaMessage)
fromMessagePtr ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then liftM (Left . kafkaRespErr) getErrno
    else do
        s <- peek realPtr
        msg <- if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then return $ Left . KafkaResponseError $ err'RdKafkaMessageT s
            else Right <$> fromMessageStorable s
        rdKafkaMessageDestroy realPtr
        return msg

fromMessageStorable :: RdKafkaMessageT -> IO KafkaMessage
fromMessageStorable s = do
    payload <- word8PtrToBS (len'RdKafkaMessageT s) (payload'RdKafkaMessageT s)
    topic   <- newForeignPtr_ (topic'RdKafkaMessageT s) >>= rdKafkaTopicName

    key <- if key'RdKafkaMessageT s == nullPtr
               then return Nothing
               else liftM Just $ word8PtrToBS (keyLen'RdKafkaMessageT s) (key'RdKafkaMessageT s)

    return $ KafkaMessage
             topic
             (partition'RdKafkaMessageT s)
             (offset'RdKafkaMessageT s)
             payload
             key

kafkaRespErr :: Errno -> KafkaError
kafkaRespErr (Errno num) = KafkaResponseError $ rdKafkaErrno2err (fromIntegral num)
{-# INLINE kafkaRespErr #-}

throwOnError :: IO (Maybe String) -> IO ()
throwOnError action = do
    m <- action
    case m of
        Just e -> throw $ KafkaError e
        Nothing -> return ()

hasError :: KafkaError -> Bool
hasError err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> False
    _ -> True
{-# INLINE hasError #-}

kafkaErrorToEither :: KafkaError -> Either KafkaError ()
kafkaErrorToEither err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> Right ()
    _ -> Left err
{-# INLINE kafkaErrorToEither #-}

offsetToInt64 :: KafkaOffset -> Int64
offsetToInt64 o = case o of
    KafkaOffsetBeginning -> -2
    KafkaOffsetEnd       -> -1
    KafkaOffset off      -> off
    KafkaOffsetStored    -> -1000
    KafkaOffsetInvalid   -> -1001
    KafkaOffsetTail i    -> -2000 - i
{-# INLINE offsetToInt64 #-}

int64ToOffset :: Int64 -> KafkaOffset
int64ToOffset o
    | o == -2    = KafkaOffsetBeginning
    | o == -1    = KafkaOffsetEnd
    | o == -1000 = KafkaOffsetStored
    | o >= 0     = KafkaOffset o
    | o <= -2000 = KafkaOffsetTail (-2000 - o)
    | otherwise  = KafkaOffsetInvalid
{-# INLINE int64ToOffset #-}
