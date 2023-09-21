{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StrictData #-}

module Disruptor where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Array.Base (unsafeRead, unsafeWrite)
import Data.Array.IO (IOArray, range)
import Data.Array.MArray
       ( MArray
       , getBounds
       , getElems
       , newArray
       , newArray_
       , newListArray
       , readArray
       )
import Data.Bits
import Data.Coerce
import Data.IORef
import Data.Ix (Ix)

------------------------------------------------------------------------
-- * Types

data RingBuffer a = RingBuffer
  { rbCapacity             :: Int
  , rbElements             :: IOArray Int a
  , rbCursor               :: IORef SequenceNumber
  , rbGatingSequences      :: IORef (IOArray Int (IORef SequenceNumber))
  , rbCachedGatingSequence :: IORef SequenceNumber
  , rbWaitStrategy         :: WaitStrategyI
  }

newtype SequenceNumber = SequenceNumber Int
  deriving newtype (Show, Eq, Ord, Num, Enum)

data WaitStrategyI = WaitStrategyI
  { notify :: IO ()
  , wait   :: IO ()
  }

spin :: Int -> IO WaitStrategyI
spin micros = return WaitStrategyI
  { notify = return ()
  , wait   = threadDelay micros
  }

mvar :: IO WaitStrategyI
mvar = do
  mvar <- newEmptyMVar
  return WaitStrategyI
    { notify = void (tryPutMVar mvar ())
    , wait   = takeMVar mvar
    }

------------------------------------------------------------------------
-- * Getters and setters

capacity :: RingBuffer a -> Int
capacity = rbCapacity

elements :: RingBuffer a -> IOArray Int a
elements = rbElements

readCursor :: RingBuffer a -> IO SequenceNumber
readCursor = readIORef . rbCursor
{-# INLINE readCursor #-}

writeCursor :: RingBuffer a -> SequenceNumber -> IO ()
writeCursor rb = writeIORef (rbCursor rb)
{-# INLINE writeCursor #-}

readGatingSequences :: RingBuffer a -> IO (IOArray Int (IORef SequenceNumber))
readGatingSequences = readIORef . rbGatingSequences

writeGatingSequences :: RingBuffer a -> IOArray Int (IORef SequenceNumber) -> IO ()
writeGatingSequences rb = writeIORef (rbGatingSequences rb)

readCachedGatingSequence :: RingBuffer a -> IO SequenceNumber
readCachedGatingSequence = readIORef . rbCachedGatingSequence

writeCachedGatingSequence :: RingBuffer a -> SequenceNumber -> IO ()
writeCachedGatingSequence rb = writeIORef (rbCachedGatingSequence rb)

------------------------------------------------------------------------
-- * Sequence number helpers

-- > quickCheck $ \(Positive i) j -> let capacity = 2^i in
--     j `mod` capacity == j Data.Bits..&. (capacity - 1)
index :: Int -> SequenceNumber -> Int
index capacity (SequenceNumber i) = i .&. indexMask
  where
    indexMask = capacity - 1
{-# INLINE index #-}

iter :: SequenceNumber -> SequenceNumber -> (SequenceNumber -> IO ()) -> IO ()
iter lo0 hi k = go (lo0 + 1)
  where
    go !lo | lo >  hi = return ()
           | lo <= hi = do
               k lo
               go (lo + 1)
{-# INLINE iter #-}

fold :: SequenceNumber -> SequenceNumber -> s -> (SequenceNumber -> s -> IO s) -> IO s
fold lo0 hi s0 k = go (lo0 + 1) s0
  where
    go !lo !s | lo >  hi = return s
              | lo <= hi = do
                  s' <- k lo s
                  go (lo + 1) s'

------------------------------------------------------------------------
-- * Create

data WaitStrategy = Spin Int | MVar

newRingBuffer :: Int -> Maybe a -> WaitStrategy -> IO (RingBuffer a)
newRingBuffer capacity mInitialValue waitStrategy
  | popCount capacity /= 1 = error "newRingBuffer: capacity must be a power of 2"
  | otherwise = RingBuffer capacity <$> elems <*> newIORef (-1) <*>
                           gatingSequences <*> newIORef (-1) <*> case waitStrategy of
                                                                   Spin micros -> spin micros
                                                                   MVar -> mvar
  where
    -- elems :: IOArray Int a
    elems = maybe (newArray_ bounds) (newArray bounds) mInitialValue

    bounds :: (Int, Int)
    bounds = (0, capacity - 1)

    gatingSequences :: IO (IORef (IOArray Int (IORef SequenceNumber)))
    gatingSequences = newIORef =<< newArray_ (0, (-1))
{-# INLINE newRingBuffer #-}

newRingBuffer_ :: Int -> WaitStrategy -> IO (RingBuffer a)
newRingBuffer_ capacity = newRingBuffer capacity Nothing
{-# INLINE newRingBuffer_ #-}

------------------------------------------------------------------------
-- * Producer

minGatingSequence :: RingBuffer a -> IO SequenceNumber
minGatingSequence rb = do
  gatingSequences <- readIORef (rbGatingSequences rb)
  (lo, hi) <- getBounds gatingSequences
  produced <- readCursor rb
  go lo hi gatingSequences produced
  where
    go :: Int -> Int -> IOArray Int (IORef SequenceNumber) -> SequenceNumber
       -> IO SequenceNumber
    go i len arr acc | i > len   = return acc
                     | otherwise = do
                        x <- readIORef =<< unsafeRead arr i
                        go (i + 1) len arr (min acc x)
{-# INLINE minGatingSequence #-}

-- | Currently available slots to write to.
size :: RingBuffer a -> IO Int
size rb = do
  consumed <- minGatingSequence rb
  produced <- readCursor rb
  return (capacity rb - coerce (produced - consumed))
  -- XXX: lengthRingBuffer = return (coerce (produced - consumed))

-- Try to return the next sequence number to write to. If `Nothing` is returned,
-- then the last consumer has not yet processed the event we are about to
-- overwrite (due to the ring buffer wrapping around) -- the callee of
-- @tryClaim@ should apply back-pressure upstream if this happens.
tryClaim :: RingBuffer a -> IO MaybeSequenceNumber
tryClaim rb = tryClaimBatch rb 1
{-# INLINE tryClaim #-}

newtype MaybeSequenceNumber = JustSN SequenceNumber
  deriving (Eq, Ord)

nothingSN :: MaybeSequenceNumber
nothingSN = JustSN (-666)

tryClaimBatch :: RingBuffer a -> Int -> IO MaybeSequenceNumber
tryClaimBatch rb n = assert (n > 0) $ do
  current <- readCursor rb
  let next = current + coerce n
      wrapPoint = next - coerce (capacity rb)
  cachedGatingSequence <- readCachedGatingSequence rb
  if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
  then do
    minSequence <- minGatingSequence rb
    writeCachedGatingSequence rb minSequence
    if (wrapPoint > minSequence)
    then return nothingSN
    else return (JustSN next)
  else return (JustSN next)
{-# INLINE tryClaimBatch #-}

writeRingBuffer :: RingBuffer a -> SequenceNumber -> a -> IO ()
writeRingBuffer rb i x = unsafeWrite (elements rb) (index (capacity rb) i) x
{-# INLINE writeRingBuffer #-}

publish :: RingBuffer a -> SequenceNumber -> IO ()
publish rb i = do
  writeCursor rb i
  notify (rbWaitStrategy rb)
{-# INLINE publish #-}

publishBatch :: RingBuffer a -> SequenceNumber -> SequenceNumber -> IO ()
publishBatch rb _lo hi = do
  writeCursor rb hi
  notify (rbWaitStrategy rb)
{-# INLINE publishBatch #-}

------------------------------------------------------------------------
-- * Consumer

addGatingSequence :: RingBuffer a -> IO (IORef SequenceNumber)
addGatingSequence rb = do
  gatingSequences <- readGatingSequences rb
  (lo, hi) <- getBounds gatingSequences
  elems <- getElems gatingSequences
  newGatingSequence <- newIORef (-1)
  gatingSequences' <- newListArray (lo, hi + 1) (elems ++ [newGatingSequence])
  writeGatingSequences rb gatingSequences'
  return newGatingSequence
{-# INLINE addGatingSequence #-}

waitFor :: RingBuffer a -> SequenceNumber -> IO SequenceNumber
waitFor rb consumed = go
  where
    go :: IO SequenceNumber
    go = do
      produced <- readCursor rb
      if consumed < produced
      then return produced
      else do
        wait (rbWaitStrategy rb)
        go
{-# INLINE waitFor #-}

readRingBuffer :: RingBuffer a -> SequenceNumber -> IO a
readRingBuffer rb want = go
  where
    go = do
      produced <- readCursor rb
      if want <= produced
      then unsafeRead (elements rb) (index (capacity rb) want)
      else do
        wait (rbWaitStrategy rb)
        go
{-# INLINE readRingBuffer #-}

------------------------------------------------------------------------
-- * Debugging

display :: RingBuffer Int -> IO ()
display rb = do
  produced <- readCursor rb
  queue <- toList rb
  putStrLn $ unlines
    [ "RingBuffer"
    , "  { rbCapacity = " ++ show (rbCapacity rb)
    , "  , elements    = " ++ show queue
    , "  , rbCursor   = " ++ show produced
    , "  }"
    ]

toList :: RingBuffer a -> IO [a]
toList rb = do
  produced <- readCursor rb
  if coerce produced < capacity rb - 1
  then goSmall 0 (coerce produced) []
  else goBig produced 1 (coerce (capacity rb)) []
  where
    goSmall lo hi acc
      | lo >  hi = return (reverse acc)
      | lo <= hi = do
          x <- readArray (elements rb) lo -- XXX: use unsafeRead?
          goSmall (lo + 1) hi (x : acc)

    goBig produced lo hi acc
      | lo >  hi = return (reverse acc)
      | lo <= hi = do
          let ix = index (capacity rb) (produced + lo)
          x <- readArray (elements rb) ix -- XXX: use unsafeRead?
          goBig produced (lo + 1) hi (x : acc)
