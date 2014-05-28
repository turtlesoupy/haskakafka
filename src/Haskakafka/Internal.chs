{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}

module Haskakafka.Internal where

import Foreign
import Foreign.C.String
import Foreign.C.Types
import System.IO
import System.Posix.IO
import System.Posix.Types

import qualified Data.Map.Strict as Map

#include <stdio.h>
#include "rdkafka.h"

{#pointer *FILE as CFilePtr -> CFile #} 

-- Helper functions

{#fun pure unsafe rd_kafka_version as ^
    {} -> `Int' #}
{#fun pure unsafe rd_kafka_version_str as ^
    {} -> `String' #}

{#fun pure unsafe rd_kafka_err2str as ^
    {enumToCInt `RdKafkaRespErrT'} -> `String' #}

-- Move where approperiate

-- Data types

data RdKafkaConfT
{#pointer *rd_kafka_conf_t as RdKafkaConfTPtr foreign -> RdKafkaConfT #}
{#fun unsafe rd_kafka_conf_new as ^
    {} -> `RdKafkaConfTPtr' #}
{#fun unsafe rd_kafka_conf_destroy as ^
    {`RdKafkaConfTPtr'} -> `()' #}
{#fun unsafe rd_kafka_conf_properties_show as ^
    {`CFilePtr'} -> `()' #}

data RdKafkaTopicConfT
{#pointer *rd_kafka_topic_conf_t as RdKafkaTopicConfTPtr foreign -> RdKafkaTopicConfT #} 
{#fun unsafe rd_kafka_topic_conf_new as ^
    {} -> `RdKafkaTopicConfTPtr' #}
{#fun unsafe rd_kafka_topic_conf_destroy as ^
    {`RdKafkaTopicConfTPtr'} -> `()' #}

data RdKafkaT
{#pointer *rd_kafka_t as RdKafkaTPtr foreign -> RdKafkaT #}
{#fun unsafe rd_kafka_destroy as ^
    {`RdKafkaTPtr'} -> `()' #}

{#fun unsafe rd_kafka_brokers_add as ^
    {`RdKafkaTPtr', `String'} -> `Int' #}

{#fun unsafe rd_kafka_dump as ^
    {`CFilePtr', `RdKafkaTPtr'} -> `()' #}

-- Enum zone
    
{#enum rd_kafka_type_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_conf_res_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_resp_err_t as ^ {underscoreToCase} deriving (Show, Eq) #}

-- Marshall / Unmarshall
enumToCInt :: Enum a => a -> CInt
enumToCInt = fromIntegral . fromEnum
cIntToEnum :: Enum a => CInt -> a
cIntToEnum = toEnum . fromIntegral

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
