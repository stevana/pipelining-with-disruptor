module DisruptorTest where

import Control.Exception (assert)
import Data.Coerce
import Data.IORef
import Test.Tasty.HUnit hiding (assert)

import Disruptor

------------------------------------------------------------------------

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

  c <- addGatingSequence xs
  writeIORef c 1

  _four <- tryClaim xs
  assertIO (_four == Just 4)
  writeRingBuffer xs 4 'e'

  cs <- toList xs
  -- print cs
  assertIO (cs == "bcde")


unit_full :: IO ()
unit_full = do
  rb <- newRingBuffer_ 2
  c <- addGatingSequence rb

  Just i <- tryClaim rb
  writeRingBuffer rb i 'a'
  publish rb i

  Just i <- tryClaim rb
  writeRingBuffer rb i 'b'
  publish rb i

  Nothing <- tryClaim rb

  i <- readIORef c
  j <- waitFor rb i
  c <- readRingBuffer rb 0
  assertIO (c == 'a')

unit_claim :: IO ()
unit_claim = do
  rb <- newRingBuffer 1 Nothing
  c <- addGatingSequence rb
  mi <- tryClaim rb
  assertIO (mi == Just 0)
  writeRingBuffer rb 0 'a'
  publish rb 0
  cu <- readCursor rb
  assertIO (cu == 0)

  mj <- tryClaim rb
  assertIO (mj == Nothing)

  i <- readIORef c
  j <- waitFor rb i

  assertIO (i == -1)
  assertIO (j == 0)

  x <- readRingBuffer rb j
  assertIO (x == 'a')
  writeIORef c j

  mk <- tryClaim rb
  assertIO (mk == Just 1)
  writeRingBuffer rb 1 'b'
  publish rb 1
  cu' <- readCursor rb
  assertIO (cu' == 1)

  i' <- readIORef c
  j' <- waitFor rb i'

  assertIO (i' == 0)
  assertIO (j' == 1)

  y <- readRingBuffer rb j'
  assertIO (y == 'b')
  writeIORef c j'

unit_example :: IO ()
unit_example = do
  rb <- newRingBuffer_ 2
  c <- addGatingSequence rb
  let batchSize = 2
  Just hi <- tryClaimBatch rb batchSize
  let lo = hi - (coerce batchSize - 1)
  assertIO (lo == 0)
  assertIO (hi == 1)
  mapM_ (\(i, c) -> writeRingBuffer rb i c) (zip [lo..hi] ['a'..])
  publish rb hi
  Nothing <- tryClaimBatch rb 1
  consumed <- readIORef c
  produced <- waitFor rb consumed
  xs <- mapM (readRingBuffer rb) [consumed + 1..produced]
  assertIO (xs == "ab")
  writeIORef c produced
  Just 2 <- tryClaimBatch rb 1
  return ()

assertIO :: HasCallStack => Bool -> IO ()
assertIO b = assert b (return ())
