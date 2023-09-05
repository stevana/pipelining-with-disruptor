{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StrictData #-}

module Disruptor where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.ST
import Data.Array.Base
       (STUArray(STUArray), getNumElements, unsafeRead, unsafeWrite)
import Data.Array.IO (IOUArray)
import Data.Array.IO.Internals
import Data.Array.MArray
       ( MArray
       , getBounds
       , getElems
       , newArray
       , newArray_
       , readArray
       , writeArray
       )
import Data.Bits
import Data.Coerce
import Data.IORef
import Data.Ix (Ix)
import GHC.Stack

------------------------------------------------------------------------

data RingBuffer a = RingBuffer
  { rbCapacity             :: Int
  , rbArray                :: IOArray Int a
  , rbCachedGatingSequence :: IORef SequenceNumber
  , rbCursor               :: IORef SequenceNumber
  , rbGatingCounters       :: GatingCounters
  }

display :: RingBuffer Int -> IO ()
display rb = do
  produced <- readCursor rb
  writeArray (rbArray rb) 0 123
  queue <- toList rb
  putStrLn $ unlines
    [ "RingBuffer"
    , "  { rbCapacity = " ++ show (rbCapacity rb)
    , "  , rbArray    = " ++ show queue
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
          x <- readArray (rbArray rb) lo -- XXX: use unsafeRead?
          goSmall (lo + 1) hi (x : acc)

    goBig produced lo hi acc
      | lo >  hi = return (reverse acc)
      | lo <= hi = do
          let ix = index (capacity rb) (produced + lo)
          x <- readArray (rbArray rb) ix -- XXX: use unsafeRead?
          goBig produced (lo + 1) hi (x : acc)

toList' :: RingBuffer a -> IO [a]
toList' rb = do
  produced <- readCursor rb
  if coerce produced < capacity rb - 1
  then go (rbArray rb) 0 (coerce produced) []
  else do
    print (produced, index (capacity rb) produced)
    xs <- go (rbArray rb) (index (capacity rb) (produced + 1) + 1) (capacity rb - 1) []
    ys <- go (rbArray rb) 0 (index (capacity rb) (produced + 1)) []
    return (xs ++ ys)
  where
    go arr i end acc | i >  end = return (reverse acc)
                     | i <= end = do
                         x <- readArray arr i -- XXX: use unsafeRead?
                         go arr (i + 1) end (x : acc)

unit_toListSmall :: IO ()
unit_toListSmall = do
  xs <- newRingBuffer_ 4
  _two <- tryClaimBatch xs 3
  assertIO (_two == Just 2)
  writeRingBuffer xs 0 'a'
  writeRingBuffer xs 1 'b'
  writeRingBuffer xs 2 'c'
  publishBatch xs 0 2
  cs <- toList xs
  assertIO (cs == "abc")

unit_toListBig :: HasCallStack => IO ()
unit_toListBig = do
  xs <- newRingBuffer_ 4
  _three <- tryClaimBatch xs 4
  assertIO (_three == Just 3)
  writeRingBuffer xs 0 'a'
  writeRingBuffer xs 1 'b'
  writeRingBuffer xs 2 'c'
  writeRingBuffer xs 3 'd'
  publishBatch xs 0 3

  c <- addGatingCounter xs
  writeCounter c 1

  _four <- tryClaim xs
  assertIO (_four == Just 4)
  writeRingBuffer xs 4 'e'

  cs <- toList xs
  print cs
  assertIO (cs == "bcde")

capacity :: RingBuffer a -> Int
capacity = rbCapacity

-- | NOTE: The first element in `GatingCounters` is the cursor of the
-- `RingBuffer`.
data GatingCounters = GatingCounters
  { gcArray     :: IOUArray Int Int
  , gcNextIndex :: IORef Int
  }
  deriving Eq

data Counter = Counter
  { snIndices        :: [Int]
  , snGatingCounters :: GatingCounters
  }

newtype SequenceNumber = SequenceNumber Int
  deriving newtype (Show, Eq, Ord, Num, Enum)

iter :: SequenceNumber -> SequenceNumber -> (SequenceNumber -> IO ()) -> IO ()
iter lo0 hi k = go (lo0 + 1)
  where
    go !lo | lo >  hi = return ()
           | lo <= hi = do
               k lo
               go (lo + 1)

fold :: SequenceNumber -> SequenceNumber -> s -> (SequenceNumber -> s -> IO s) -> IO s
fold lo0 hi s0 k = go (lo0 + 1) s0
  where
    go !lo !s | lo >  hi = return s
              | lo <= hi = do
                  s' <- k lo s
                  go (lo + 1) s'

-- > quickCheck $ \(Positive i) j -> let capacity = 2^i in
--     j `mod` capacity == j Data.Bits..&. (capacity - 1)
index :: Int -> SequenceNumber -> Int
index capacity (SequenceNumber i) = i .&. indexMask
  where
    indexMask = capacity - 1

readCounter :: Counter -> IO SequenceNumber
readCounter (Counter ixs0 (GatingCounters arr _next)) =
  go ixs0 maxBound
  where
    go []         acc = return (coerce acc)
    go (ix : ixs) acc = do
      x <- unsafeRead arr ix
      go ixs $! min x acc

writeCounter :: Counter -> SequenceNumber -> IO ()
writeCounter (Counter ixs0 (GatingCounters arr _next)) i =
  go ixs0
  where
    go []         = return ()
    go (ix : ixs) = do
      unsafeWrite arr ix (coerce i)
      go ixs

combineCounters :: Counter -> Counter -> Counter
combineCounters (Counter ixs arr) (Counter ixs' arr') = -- XXX: assert (arr == arr') $
  Counter (ixs ++ ixs') arr

readCursor :: RingBuffer a -> IO SequenceNumber
readCursor rb = readIORef (rbCursor rb)
  -- readCounter (Counter [0] (rbGatingCounters rb))

writeCursor :: RingBuffer a -> SequenceNumber -> IO ()
writeCursor rb = atomicWriteIORef (rbCursor rb)
  -- writeCounter (Counter [0] (rbGatingCounters rb)) i

readCachedGatingSequence :: RingBuffer a -> IO SequenceNumber
readCachedGatingSequence rb = readIORef (rbCachedGatingSequence rb)

writeCachedGatingSequence :: RingBuffer a -> SequenceNumber -> IO ()
writeCachedGatingSequence rb i = writeIORef (rbCachedGatingSequence rb) i

------------------------------------------------------------------------

newRingBuffer :: Int -> Maybe a -> IO (RingBuffer a)
newRingBuffer capacity mInitialValue
  | popCount capacity /= 1 = error "newRingBuffer: capacity must be a power of 2"
  | otherwise = RingBuffer capacity <$> array <*> newIORef 0 <*> newIORef (-1) <*> gatingCounters
  where
    array = maybe (newArray_ bounds) (newArray bounds) mInitialValue
    bounds = (0, capacity - 1)
    gatingCounters = GatingCounters <$> newArray (0, 5) (-1) <*> newIORef 0
    -- ^ XXX: hardcoded... can this be statically computed at deploy time?

newRingBuffer_ :: Int -> IO (RingBuffer a)
newRingBuffer_ capacity = newRingBuffer capacity Nothing

-- | Currently available slots to write to.
size :: RingBuffer a -> IO Int
size rb = do
  consumed <- minGatingSequence rb
  produced <- readCursor rb
  return (capacity rb - coerce (produced - consumed))
  -- XXX: lengthRingBuffer = return (coerce (produced - consumed))

claim :: RingBuffer a -> IO SequenceNumber
claim rb = claimBatch rb 1

claimBatch :: RingBuffer a -> Int -> IO SequenceNumber
claimBatch rb n = assert (n > 0 && n <= capacity rb) $ do
  error "BUG"
  current <- readCursor rb
  let nextSequence :: SequenceNumber
      nextSequence = current + coerce n

      wrapPoint :: SequenceNumber
      wrapPoint = nextSequence - coerce (capacity rb)

  writeCursor rb nextSequence
  cachedGatingSequence <- readCachedGatingSequence rb

  when (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) $
    waitForConsumers wrapPoint

  return nextSequence
  where
    -- XXX: some bug here?
    waitForConsumers :: SequenceNumber -> IO ()
    waitForConsumers wrapPoint = go
      where
        go :: IO ()
        go = do
          gatingSequence <- minGatingSequence rb
          if wrapPoint > gatingSequence
          then do
            threadDelay 1000000
            print "WAITING"
            go -- SPIN
          else writeCachedGatingSequence rb gatingSequence

-- Try to return the next sequence number to write to. If `Nothing` is returned,
-- then the last consumer has not yet processed the event we are about to
-- overwrite (due to the ring buffer wrapping around) -- the callee of
-- @tryClaim@ should apply back-pressure upstream if this happens.
tryClaim :: RingBuffer a -> IO (Maybe SequenceNumber)
tryClaim rb = tryClaimBatch rb 1

tryClaimBatch :: RingBuffer a -> Int -> IO (Maybe SequenceNumber)
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
    then return Nothing
    else return (Just $! next)
  else return (Just $! next)

writeRingBuffer :: RingBuffer a -> SequenceNumber -> a -> IO ()
writeRingBuffer rb i x = writeArray (rbArray rb) (index (capacity rb) i) x

publish :: RingBuffer a -> SequenceNumber -> IO ()
publish rb i = writeCursor rb i

publishBatch :: RingBuffer a -> SequenceNumber -> SequenceNumber -> IO ()
publishBatch rb _lo hi = writeCursor rb hi

readRingBuffer :: RingBuffer a -> SequenceNumber -> IO a
readRingBuffer rb want = do
  produced <- readCursor rb
  if want <= produced
  then readArray (rbArray rb) (index (capacity rb) want)
  else do
    threadDelay 100
    -- XXX: non-blocking interface?
    readRingBuffer rb want

addGatingCounter :: RingBuffer a -> IO Counter
addGatingCounter rb = do
  let gc = rbGatingCounters rb
  ix <- readIORef (gcNextIndex gc)
  writeIORef (gcNextIndex gc) (ix + 1)
  return (Counter [ix] gc)

waitFor :: RingBuffer a -> SequenceNumber -> IO SequenceNumber
waitFor rb consumed = go
  where
    go = do
      produced <- readCursor rb
      if consumed < produced
      then return produced
      else do
        threadDelay 100 -- NOTE: removing the sleep seems to cause
                      -- non-termination...
        go -- SPIN
        -- ^ XXX: Other wait strategies could be implemented here, e.g. we could
        -- try to recurse immediately here, and if there's no work after a
        -- couple of tries go into a takeMTVar sleep waiting for a producer to
        -- wake us up.

------------------------------------------------------------------------

minGatingSequence :: RingBuffer a -> IO SequenceNumber
minGatingSequence rb = do
  let GatingCounters arr nextIx = rbGatingCounters rb
  len <- readIORef nextIx
  produced <- readCursor rb
  go 0 (len - 1) arr (coerce produced)
  where
    go i len arr acc | i > len   = return (coerce acc)
                     | otherwise = do
                        -- x <- unsafeRead arr i
                        x <- readArray arr i
                        go (i + 1) len arr (min acc x)


minGatingSequence' :: GatingCounters -> IO SequenceNumber
minGatingSequence' (GatingCounters arr nextIx) = do
  len <- readIORef nextIx
  go 0 (len - 1) maxBound
  where
    go i len acc | i > len   = return (coerce acc)
                 | otherwise = do
                     -- x <- unsafeRead arr i
                     x <- readArray arr i
                     go (i + 1) len (min acc x)


------------------------------------------------------------------------

unit_full :: IO ()
unit_full = do
  rb <- newRingBuffer 2 Nothing
  c <- addGatingCounter rb

  i <- claim rb
  putStr "claim: "
  print i
  writeRingBuffer rb i 'a'
  publish rb i

  i <- claim rb
  putStr "claim: "
  print i
  writeRingBuffer rb i 'b'
  publish rb i

  mi <- tryClaim rb
  putStr "claim: "
  print mi

  i <- readCounter c
  j <- waitFor rb i
  print (i, j)
  -- c <- readRingBuffer rb 0
  -- print c

  -- writeRingBuffer rb i 'c'
  -- publish rb i

  -- j <- waitFor rb 1
  -- print j
  -- c <- readRingBuffer rb 0
  -- print c

unit_claim :: IO ()
unit_claim = do
  rb <- newRingBuffer 1 Nothing
  c <- addGatingCounter rb
  mi <- tryClaim rb
  assertIO (mi == Just 0)
  writeRingBuffer rb 0 'a'
  publish rb 0
  cu <- readCursor rb
  assertIO (cu == 0)

  mj <- tryClaim rb
  assertIO (mj == Nothing)

  i <- readCounter c
  j <- waitFor rb i

  assertIO (i == -1)
  assertIO (j == 0)

  x <- readRingBuffer rb j
  assertIO (x == 'a')
  writeCounter c j

  mk <- tryClaim rb
  assertIO (mk == Just 1)
  writeRingBuffer rb 1 'b'
  publish rb 1
  cu' <- readCursor rb
  assertIO (cu' == 1)

  i' <- readCounter c
  j' <- waitFor rb i'

  assertIO (i' == 0)
  assertIO (j' == 1)

  y <- readRingBuffer rb j'
  assertIO (y == 'b')
  writeCounter c j'

t = do
  unit_claim

assertIO :: HasCallStack => Bool -> IO ()
assertIO b = assert b (return ())
