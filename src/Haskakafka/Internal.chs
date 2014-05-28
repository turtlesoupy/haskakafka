{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}

module Haskakafka.Internal where

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

{#pointer *FILE as CFilePtr -> CFile #} 
{#pointer *size_t as CSizePtr -> CSize #}

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

-- Move where approperiate

-- Data types

data RdKafkaConfT
{#pointer *rd_kafka_conf_t as RdKafkaConfTPtr foreign -> RdKafkaConfT #}
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

data RdKafkaTopicConfT
{#pointer *rd_kafka_topic_conf_t as RdKafkaTopicConfTPtr foreign -> RdKafkaTopicConfT #} 
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

data RdKafkaT
{#pointer *rd_kafka_t as RdKafkaTPtr foreign -> RdKafkaT #}
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

{#fun unsafe rd_kafka_dump as ^
    {`CFilePtr', `RdKafkaTPtr'} -> `()' #}

data RdKafkaTopicT
{#pointer *rd_kafka_topic_t as RdKafkaTopicTPtr foreign -> RdKafkaTopicT #}
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
