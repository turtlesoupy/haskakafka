{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}

module Haskakafka.Internal where

import Control.Applicative
import Control.Monad
import Data.Word
import Foreign
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import System.IO
import System.Posix.IO
import System.Posix.Types

import Haskakafka.InternalEnum

import qualified Data.Map.Strict as Map

#include "rdkafka.h"

type CInt64T = {#type int64_t #}
type CInt32T = {#type int32_t #}

{#pointer *FILE as CFilePtr -> CFile #} 
{#pointer *size_t as CSizePtr -> CSize #}

type Word8Ptr = Ptr Word8
type CCharBufPointer  = Ptr CChar

-- Helper functions
{#fun pure unsafe rd_kafka_version as ^
    {} -> `Int' #}

{#fun pure unsafe rd_kafka_version_str as ^
    {} -> `String' #}

{#fun pure unsafe rd_kafka_err2str as ^
    {enumToCInt `RdKafkaRespErrT'} -> `String' #}

{#fun pure unsafe rd_kafka_errno2err as ^
    {`Int'} -> `RdKafkaRespErrT' cIntToEnum #}


kafkaErrnoString :: IO (String)
kafkaErrnoString = do
    (Errno num) <- getErrno 
    return $ rdKafkaErr2str $ rdKafkaErrno2err (fromIntegral num)

-- Kafka Pointer Types
data RdKafkaConfT
{#pointer *rd_kafka_conf_t as RdKafkaConfTPtr foreign -> RdKafkaConfT #}

data RdKafkaTopicConfT
{#pointer *rd_kafka_topic_conf_t as RdKafkaTopicConfTPtr foreign -> RdKafkaTopicConfT #} 

data RdKafkaT
{#pointer *rd_kafka_t as RdKafkaTPtr foreign -> RdKafkaT #}

data RdKafkaTopicT
{#pointer *rd_kafka_topic_t as RdKafkaTopicTPtr foreign -> RdKafkaTopicT #}

data RdKafkaMessageT = RdKafkaMessageT 
    { err'RdKafkaMessageT :: RdKafkaRespErrT
    , partition'RdKafkaMessageT :: Int
    , len'RdKafkaMessageT :: Int
    , keyLen'RdKafkaMessageT :: Int
    , offset'RdKafkaMessageT :: Int64
    , payload'RdKafkaMessageT :: Word8Ptr
    , key'RdKafkaMessageT :: Word8Ptr
    }
    deriving (Show, Eq)
    
instance Storable RdKafkaMessageT where
    alignment _ = {#alignof rd_kafka_message_t#}
    sizeOf _ = {#sizeof rd_kafka_message_t#}
    peek p = RdKafkaMessageT
        <$> liftM cIntToEnum  ({#get rd_kafka_message_t->err #} p)
        <*> liftM fromIntegral ({#get rd_kafka_message_t->partition #} p)
        <*> liftM fromIntegral ({#get rd_kafka_message_t->len #} p)
        <*> liftM fromIntegral ({#get rd_kafka_message_t->key_len#} p)
        <*> liftM fromIntegral ({#get rd_kafka_message_t->offset#} p)
        <*> liftM castPtr ({#get rd_kafka_message_t->payload#} p)
        <*> liftM castPtr ({#get rd_kafka_message_t->key#} p)
    poke p x = undefined

{#pointer *rd_kafka_message_t as RdKafkaMessageTPtr foreign -> RdKafkaMessageT #}

-- rd_kafka_conf
{#fun unsafe rd_kafka_conf_new as ^
    {} -> `RdKafkaConfTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_conf_destroy"
    rdKafkaConfDestroy :: FunPtr (Ptr RdKafkaConfT -> IO ())

{#fun unsafe rd_kafka_conf_dup as ^
    {`RdKafkaConfTPtr'} -> `RdKafkaConfTPtr' #}

newRdKafkaConfT :: IO RdKafkaConfTPtr
newRdKafkaConfT = do
    ret <- rdKafkaConfNew
    addForeignPtrFinalizer rdKafkaConfDestroy ret
    return ret

{#fun unsafe rd_kafka_conf_dump as ^
    {`RdKafkaConfTPtr', castPtr `CSizePtr'} -> `Ptr CString' id #}

{#fun unsafe rd_kafka_conf_dump_free as ^
    {id `Ptr CString', cIntConv `CSize'} -> `()' #}

{#fun unsafe rd_kafka_conf_properties_show as ^
    {`CFilePtr'} -> `()' #}

-- rd_kafka_topic_conf
{#fun unsafe rd_kafka_topic_conf_new as ^
    {} -> `RdKafkaTopicConfTPtr' #}
{#fun unsafe rd_kafka_topic_conf_dup as ^
    {`RdKafkaTopicConfTPtr'} -> `RdKafkaTopicConfTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_conf_destroy"
    rdKafkaTopicConfDestroy :: FunPtr (Ptr RdKafkaTopicConfT -> IO ())

newRdKafkaTopicConfT :: IO RdKafkaTopicConfTPtr
newRdKafkaTopicConfT = do
    ret <- rdKafkaTopicConfNew
    addForeignPtrFinalizer rdKafkaTopicConfDestroy ret
    return ret

{#fun unsafe rd_kafka_topic_conf_dump as ^
    {`RdKafkaTopicConfTPtr', castPtr `CSizePtr'} -> `Ptr CString' id #}

-- rd_kafka
{#fun unsafe rd_kafka_new as ^
    {enumToCInt `RdKafkaTypeT', `RdKafkaConfTPtr', id `CCharBufPointer', cIntConv `CSize'} 
    -> `RdKafkaTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_destroy"
    rdKafkaDestroy :: FunPtr (Ptr RdKafkaT -> IO ())

nErrorBytes ::  Int
nErrorBytes = 1024 * 8

newRdKafkaT :: RdKafkaTypeT -> RdKafkaConfTPtr -> IO (Either String RdKafkaTPtr)
newRdKafkaT kafkaType confPtr = 
    allocaBytes nErrorBytes $ \charPtr -> do
        duper <- rdKafkaConfDup confPtr
        ret <- rdKafkaNew kafkaType duper charPtr (fromIntegral nErrorBytes)
        withForeignPtr ret $ \realPtr -> do
            if realPtr == nullPtr then peekCString charPtr >>= return . Left
            else do
                addForeignPtrFinalizer rdKafkaDestroy ret
                return $ Right ret

{#fun unsafe rd_kafka_brokers_add as ^
    {`RdKafkaTPtr', `String'} -> `Int' #}

{#fun unsafe rd_kafka_consume_start as rdKafkaConsumeStartInternal
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', cIntConv `CInt64T'} -> `Int' #}

rdKafkaConsumeStart :: RdKafkaTopicTPtr -> Int -> Int64 -> IO (Maybe String)
rdKafkaConsumeStart topicPtr partition offset = do
    i <- rdKafkaConsumeStartInternal topicPtr (fromIntegral partition) (fromIntegral offset)
    case i of 
        -1 -> kafkaErrnoString >>= return . Just
        _ -> return Nothing
{#fun unsafe rd_kafka_consume_stop as rdKafkaConsumeStopInternal
    {`RdKafkaTopicTPtr', cIntConv `CInt32T'} -> `Int' #}

{#fun unsafe rd_kafka_consume as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int'} -> `RdKafkaMessageTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_message_destroy"
    rdKafkaMessageDestroy :: FunPtr (Ptr RdKafkaMessageT -> IO ())

rdKafkaConsumeStop :: RdKafkaTopicTPtr -> Int -> IO (Maybe String)
rdKafkaConsumeStop topicPtr partition = do
    i <- rdKafkaConsumeStopInternal topicPtr (fromIntegral partition)
    case i of 
        -1 -> kafkaErrnoString >>= return . Just
        _ -> return Nothing

{#fun unsafe rd_kafka_dump as ^
    {`CFilePtr', `RdKafkaTPtr'} -> `()' #}


-- rd_kafka_topic
{#fun unsafe rd_kafka_topic_new as ^
    {`RdKafkaTPtr', `String', `RdKafkaTopicConfTPtr'} -> `RdKafkaTopicTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_destroy"
    rdKafkaTopicDestroy :: FunPtr (Ptr RdKafkaTopicT -> IO ())

newRdKafkaTopicT :: RdKafkaTPtr -> String -> RdKafkaTopicConfTPtr -> IO (Either String RdKafkaTopicTPtr)
newRdKafkaTopicT kafkaPtr topic topicConfPtr = do
    duper <- rdKafkaTopicConfDup topicConfPtr
    ret <- rdKafkaTopicNew kafkaPtr topic duper
    withForeignPtr ret $ \realPtr ->
        if realPtr == nullPtr then kafkaErrnoString >>= return . Left
        else do
            addForeignPtrFinalizer rdKafkaTopicDestroy ret
            return $ Right ret


-- Marshall / Unmarshall
enumToCInt :: Enum a => a -> CInt
enumToCInt = fromIntegral . fromEnum
cIntToEnum :: Enum a => CInt -> a
cIntToEnum = toEnum . fromIntegral
cIntConv :: (Integral a, Num b) =>  a -> b
cIntConv = fromIntegral

-- Handle -> File descriptor

foreign import ccall "" fdopen :: Fd -> CString -> IO (Ptr CFile)
 
handleToCFile :: Handle -> String -> IO (CFilePtr)
handleToCFile h m =
 do iomode <- newCString m
    fd <- handleToFd h
    fdopen fd iomode
 
c_stdin :: IO CFilePtr
c_stdin = handleToCFile stdin "r"
c_stdout :: IO CFilePtr
c_stdout = handleToCFile stdout "w"
c_stderr :: IO CFilePtr
c_stderr = handleToCFile stderr "w"
