{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}

module RingBufferClass where

import Control.Concurrent
import Data.IORef
import Data.String

import Counter
import Disruptor (RingBuffer, SequenceNumber)
import qualified Disruptor

------------------------------------------------------------------------

newtype Label = Label String
  deriving (Eq, Show, IsString, Semigroup, Monoid)

data Input  a = Input  !a | EndOfStream
data Output b = Output !b | NoOutput

instance Show a => Show (Input a) where
  show (Input x)   = escape $ show x
  show EndOfStream = "EndOfStream"

instance Show b => Show (Output b) where
  show (Output y) = escape $ show y
  show NoOutput   = "NoOutput"

escape :: String -> String
escape []         = []
escape ('\r' : cs) = '\\' : '\r' : escape cs
escape ('\\' : '"' : cs) = '\\' : '"' : escape cs
escape ('"' : cs) = '\\' : '"' : escape cs
escape ('-' : cs) = '\\' : '-' : escape cs
escape ('[' : cs) = '\\' : '[' : escape cs
escape (']' : cs) = '\\' : ']' : escape cs
escape (c   : cs) = c : escape cs

------------------------------------------------------------------------

class HasRB a where
  data RB a :: *
  new           :: Label -> Int -> IO (RB a)
  cursor        :: RB a -> IORef SequenceNumber
  label         :: RB a -> [Label]
  tryClaim      :: RB a -> IO (Maybe SequenceNumber)
  tryClaimBatch :: RB a -> Int -> IO (Maybe SequenceNumber)
  write         :: RB a -> SequenceNumber -> a -> IO ()
  commit        :: RB a -> SequenceNumber -> IO ()
  commitBatch   :: RB a -> SequenceNumber -> SequenceNumber -> IO ()
  waitFor       :: RB a -> SequenceNumber -> IO SequenceNumber
  readCursor    :: RB a -> IO SequenceNumber
  tryRead       :: RB a -> SequenceNumber -> IO a
  addConsumer   :: RB a -> IO Counter
  toList        :: RB a -> IO [a]

claim :: HasRB a => RB a -> Int -> IO SequenceNumber
claim rb millis = do
  mi <- tryClaim rb
  case mi of
    Nothing -> threadDelay millis >> claim rb millis
    Just i  -> return i

claimBatch :: HasRB a => RB a -> Int -> Int -> IO SequenceNumber
claimBatch rb n millis = do
  mi <- tryClaimBatch rb n
  case mi of
    Nothing -> threadDelay millis >> claimBatch rb n millis
    Just i  -> return i

instance HasRB (Input a) where
  data RB (Input a)             = RBInput [Label] (RingBuffer (Input a))
  new l n                       = RBInput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBInput _l rb) = Disruptor.rbCursor rb
  label         (RBInput l _rb) = l
  tryClaim      (RBInput _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBInput _l rb) = Disruptor.tryClaimBatch rb
  write         (RBInput _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBInput _l rb) = Disruptor.publish rb
  commitBatch   (RBInput _l rb) = Disruptor.publishBatch rb
  waitFor       (RBInput _l rb) = Disruptor.waitFor rb
  readCursor    (RBInput _l rb) = Disruptor.readCursor rb
  tryRead       (RBInput _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBInput _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBInput _l rb) = Disruptor.toList rb

instance HasRB (Output a) where
  data RB (Output a)          = RBOutput [Label] (RingBuffer (Output a))
  new l n                     = RBOutput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBOutput _l rb) = Disruptor.rbCursor rb
  label         (RBOutput  l _rb) = l
  tryClaim      (RBOutput _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBOutput _l rb) = Disruptor.tryClaimBatch rb
  write         (RBOutput _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBOutput _l rb) = Disruptor.publish rb
  commitBatch   (RBOutput _l rb) = Disruptor.publishBatch rb
  waitFor       (RBOutput _l rb) = Disruptor.waitFor rb
  readCursor    (RBOutput _l rb) = Disruptor.readCursor rb
  tryRead       (RBOutput _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBOutput _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBOutput _l rb) = Disruptor.toList rb

instance HasRB String where
  data RB String           = RB [Label] (RingBuffer String)
  new l n                  = RB [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RB _l rb) = Disruptor.rbCursor rb
  label         (RB  l _rb) = l
  tryClaim      (RB _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RB _l rb) = Disruptor.tryClaimBatch rb
  write         (RB _l rb) = Disruptor.writeRingBuffer rb
  commit        (RB _l rb) = Disruptor.publish rb
  commitBatch   (RB _l rb) = Disruptor.publishBatch rb
  waitFor       (RB _l rb) = Disruptor.waitFor rb
  readCursor    (RB _l rb) = Disruptor.readCursor rb
  tryRead       (RB _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RB _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RB _l rb) = Disruptor.toList rb

instance (HasRB a, HasRB b) => HasRB (a, b) where
  data RB (a, b) = RBPair [Label] (RB a) (RB b)
  new l n = RBPair [l] <$> new l n <*> new l n
  cursor        (RBPair _l xs ys) = undefined
  label         (RBPair  l _xs _ys) = l
  tryClaim rb = undefined
  tryClaimBatch rb = undefined
  addConsumer (RBPair _l xs ys) = do
    c <- addConsumer xs
    d <- addConsumer ys
    return (combineCounters c d)
  waitFor (RBPair _l xs ys) i = do
    hi <- waitFor xs i
    hj <- waitFor ys i
    return (min hi hj)
  tryRead (RBPair _l xs ys) i = do
    x <- tryRead xs i
    y <- tryRead ys i
    return (x, y)
  write = undefined
  commit = undefined
  commitBatch = undefined
  readCursor = undefined
  toList (RBPair _l xs ys) = do
    xs' <- toList xs
    ys' <- toList ys
    return (zip xs' ys')

instance  HasRB (Either a b) where
  data RB (Either a b)        = RBEither [Label] (RingBuffer (Either a b))
  new l n                     = RBEither [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBEither _l rb) = Disruptor.rbCursor rb
  label         (RBEither  l _rb) = l
  tryClaim      (RBEither _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBEither _l rb) = Disruptor.tryClaimBatch rb
  write         (RBEither _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBEither _l rb) = Disruptor.publish rb
  commitBatch   (RBEither _l rb) = Disruptor.publishBatch rb
  waitFor       (RBEither _l rb) = Disruptor.waitFor rb
  readCursor    (RBEither _l rb) = Disruptor.readCursor rb
  tryRead       (RBEither _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBEither _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBEither _l rb) = Disruptor.toList rb
  {-
  data RB (Either a b) = RBEither (RB a) (RB b)
  new n = RBEither <$> new n <*> new n
  tryClaim (RBEither xs ys) (Left x) = tryClaim xs x
  tryClaim (RBEither xs ys) (Right y) = tryClaim ys y
  addConsumer (RBEither xs ys) = do
    c <- addConsumer xs
    d <- addConsumer ys
    return (Disruptor.combineCounters c d)
  waitFor (RBEither xs ys) i = undefined
  write (RBEither xs ys) i (Left  x) = write xs i x
  write (RBEither xs ys) i (Right y) = write ys i y
  commit (RBEither xs ys) (Left x) = commit xs x
  commit (RBEither xs ys) (Right y) = commit ys y
  tryRead (RBEither xs ys) i = undefined
-}
