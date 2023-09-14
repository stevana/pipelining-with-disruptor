{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}

module RingBufferClass where

import Control.Concurrent
import Data.Bits
import Data.Coerce
import Data.Kind (Type)
import Data.String

import Counter
import Disruptor (RingBuffer, SequenceNumber)
import qualified Disruptor
import Sharding

------------------------------------------------------------------------

newtype Label = Label String
  deriving (Eq, Ord, Show, IsString, Semigroup, Monoid)

data Input  a = Input  a | EndOfStream
data Output b = Output b | NoOutput

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
  data RB a :: Type
  new           :: Label -> Int -> IO (RB a)
  cursor        :: RB a -> Counter
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
  toListSharded :: RB a -> Sharding -> IO [a]

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

instance HasRB a => HasRB (Sharded a) where
  data RB (Sharded a) = RBShard [Label] Sharding Sharding (RB a) (RB a)
  new l n = error "new, RBShard: shouldn't be created explicitly"
  cursor (RBShard _l _s1 _s2 xs ys) = cursor xs `combineCounters` cursor ys
  label (RBShard  l _s1 _s2 _xs _ys) = l
  tryClaim (RBShard _l _s1 _s2 xs ys) = undefined
  tryClaimBatch (RBShard _l _s1 _s2 xs ys) n = undefined
  addConsumer (RBShard _l _s1 _s2 xs ys) = do
    c <- addConsumer xs
    d <- addConsumer ys
    return (combineCounters c d)
  waitFor (RBShard _l s1 s2 xs ys) i = do
    if partition i s1
    then waitFor xs i
    else if partition i s2
         then waitFor ys i
         else error "waitFor, RBShard"
  tryRead (RBShard _l s1 s2 xs ys) i = do
    if partition i s1
    then coerce (tryRead xs i)
    else if partition i s2
         then coerce (tryRead ys i)
         else error "tryRead, RBShard"
  write (RBShard _l s1 s2 xs ys) i x = do
    undefined
    -- write xs i x
    -- write ys i y
  commit (RBShard _l s1 s2 xs ys) i = do
    undefined
  commitBatch (RBShard _l s1 s2 xs ys) lo hi = do
    undefined
  readCursor (RBShard _l s1 s2 xs ys) = do
    i <- readCursor xs
    j <- readCursor ys
    return (max i j)
  toList (RBShard _l s1 s2 xs ys) = do
    xs' <- toListSharded xs s1
    ys' <- toListSharded ys s2
    return (interleave xs' ys')
    where
      interleave [] ys = map Sharded ys
      interleave xs [] = map Sharded xs
      interleave (x : xs) (y : ys) = Sharded x : Sharded y : interleave xs ys

instance (HasRB a, HasRB b) => HasRB (a, b) where
  data RB (a, b) = RBPair [Label] (RB a) (RB b)
  new l n = RBPair [l] <$> new l n <*> new l n
  cursor (RBPair _l xs ys) = cursor xs `combineCounters` cursor ys
  label (RBPair  l _xs _ys) = l
  tryClaim (RBPair _l xs ys) = do
    mi <- tryClaim xs
    mj <- tryClaim ys
    return (min mi mj) -- XXX: assert mi == mj?
  tryClaimBatch (RBPair _l xs ys) n = do
    mi <- tryClaimBatch xs n
    mj <- tryClaimBatch ys n
    return (min mi mj)
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
  write (RBPair _l xs ys) i (x, y) = do
    write xs i x
    write ys i y
  commit (RBPair _l xs ys) i = do
    commit xs i
    commit ys i
  commitBatch (RBPair _l xs ys) lo hi = do
    commitBatch xs lo hi
    commitBatch ys lo hi
  readCursor (RBPair _l xs ys) = do
    i <- readCursor xs
    j <- readCursor ys
    return (min i j)
  toList (RBPair _l xs ys) = do
    xs' <- toList xs
    ys' <- toList ys
    return (zip xs' ys')
  toListSharded (RBPair _l xs ys) s = do
    xs' <- toListSharded xs s
    ys' <- toListSharded ys s
    return (zip xs' ys')

instance HasRB (Input a) where
  data RB (Input a)             = RBInput [Label] (RingBuffer (Input a))
  new l n                       = RBInput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBInput _l rb) = makeCounter (Disruptor.rbCursor rb)
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
  toListSharded (RBInput _l rb) = toListSharded_ rb

instance HasRB (Output a) where
  data RB (Output a)          = RBOutput [Label] (RingBuffer (Output a))
  new l n                     = RBOutput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBOutput _l rb) = makeCounter (Disruptor.rbCursor rb)
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
  toListSharded (RBOutput _l rb) = toListSharded_ rb

instance HasRB String where
  data RB String           = RB [Label] (RingBuffer String)
  new l n                  = RB [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RB _l rb) = makeCounter (Disruptor.rbCursor rb)
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
  toListSharded (RB _l rb) = toListSharded_ rb

instance  HasRB (Either a b) where
  data RB (Either a b)        = RBEither [Label] (RingBuffer (Either a b))
  new l n                     = RBEither [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBEither _l rb) = makeCounter (Disruptor.rbCursor rb)
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
  toListSharded (RBEither _l rb) = toListSharded_ rb
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
