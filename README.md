# Haskakafka

Kafka bindings for Haskell backed by the 
librdkafka C module (https://github.com/edenhill/librdkafka). See below for 

These are meant to be a proof of concept and need some cleanup before production use.

# Installation

## Installing librdkafka

Although librdkafka is available on many platforms, most of
the distribution packages are too old to support haskakafka.
As such, we suggest you install from the source:

    git clone https://github.com/edenhill/librdkafka
    cd librdkafka
    ./configure
    make && sudo make install

On OSX, the C++ bindings were failing for me. If this is the case, just install the C bindings alone. 

    cd librdkafka/src
    make && sudo make install

## Installing Kafka

The full Kafka guide is at http://kafka.apache.org/documentation.html#quickstart

## Installing Haskakafka

Since haskakafka uses `c2hs` to generate C bindings, you may need to 
explicitly install `c2hs` somewhere on your path (i.e. outside of a sandbox).
To do so, run:
    
    cabal install c2hs

Afterwards installation should work, so go for

    cabal install haskakafka
