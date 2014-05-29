# Haskakafka

Kafka bindings for Haskell backed by the 
librdkafka C module (https://github.com/edenhill/librdkafka). 

Currently limited to consumers only.

## Installing librtkafka

Simple instructions:
    git clone https://github.com/edenhill/librdkafka
    cd librdkafka
    ./configure
    make && sudo make install

On OSX, the C++ bindings were failing for me. If this is the case, just install the C bindings alone. 
    cd lbirdkafka/src
    make && sudo make install

## Installing Kafka

The full Kafka guide is at http://kafka.apache.org/documentation.html#quickstart
