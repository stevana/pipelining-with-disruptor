{-# LANGUAGE GADTs #-}

module QueueDeployment where

import Control.Exception (finally)
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import GHC.Num.Natural
import Test.QuickCheck.Monadic

import ModelIO

------------------------------------------------------------------------

qUEUE_SIZE :: Natural
qUEUE_SIZE = 2^16

deploy :: P a b -> TBQueue a -> IO (TBQueue b)
deploy Id         xs = return xs
deploy (f :>>> g) xs = deploy g =<< deploy f xs
deploy (Map f)    xs = deploy (MapM (return . f)) xs
deploy (MapM f)   xs = do
  ys <- newTBQueueIO qUEUE_SIZE
  _pid <- forkIO $ forever $ do
    x <- atomically (readTBQueue xs)
    y <- f x
    atomically (writeTBQueue ys y)
  return ys
deploy (f :&&& g) xs = do
  xs1 <- newTBQueueIO qUEUE_SIZE
  xs2 <- newTBQueueIO qUEUE_SIZE
  _pid <- forkIO $ forever $ do
    x <- atomically (readTBQueue xs)
    atomically $ do
      writeTBQueue xs1 x
      writeTBQueue xs2 x
  ys <- deploy f xs1
  zs <- deploy g xs2
  yzs <- newTBQueueIO qUEUE_SIZE
  _pid <- forkIO $ forever $ do
    y <- atomically (readTBQueue ys)
    z <- atomically (readTBQueue zs)
    atomically (writeTBQueue yzs (y, z))
  return yzs
deploy (Shard f) xs = do
  xsEven <- newTBQueueIO qUEUE_SIZE
  xsOdd  <- newTBQueueIO qUEUE_SIZE
  _pid   <- forkIO $ shard xs xsEven xsOdd
  ysEven <- deploy f xsEven
  ysOdd  <- deploy f xsOdd
  ys     <- newTBQueueIO qUEUE_SIZE
  _pid   <- forkIO $ merge ysEven ysOdd ys
  return ys
  where
    shard :: TBQueue a -> TBQueue a -> TBQueue a -> IO ()
    shard  qIn qEven qOdd = do
      atomically (readTBQueue qIn >>= writeTBQueue qEven)
      shard qIn qOdd qEven

    merge :: TBQueue a -> TBQueue a -> TBQueue a -> IO ()
    merge qEven qOdd qOut = do
      atomically (readTBQueue qEven >>= writeTBQueue qOut)
      merge qOdd qEven qOut

deploy _ _ = error "not implemented"

example' :: [Int] -> IO [(Int, Bool)]
example' xs0 = do
  xs <- newTBQueueIO qUEUE_SIZE
  mapM_ (atomically . writeTBQueue xs) xs0
  ys <- deploy (Id :&&& Map even) xs
  replicateM (length xs0) (atomically (readTBQueue ys))

prop_commute :: Eq b => P a b -> [a] -> PropertyM IO ()
prop_commute p xs = do
  ys <- run $ do
    qxs <- newTBQueueIO qUEUE_SIZE
    mapM_ (atomically . writeTBQueue qxs) xs
    qys <- deploy p qxs
    replicateM (length xs) (atomically (readTBQueue qys))
  ys' <- run (model p xs)
  assert (ys == ys')

------------------------------------------------------------------------

queueSleepSeq :: P () ()
queueSleepSeq =
  MapM $ \() -> do
    ()       <- threadDelay 250000
    ((), ()) <- (,) <$> threadDelay 250000 <*> threadDelay 250000
    ()       <- threadDelay 250000
    return ()

queueSleep :: P () ()
queueSleep =
  MapM (const (threadDelay 250000)) :&&& MapM (const (threadDelay 250000)) :>>>
  MapM (const (threadDelay 250000)) :>>>
  MapM (const (threadDelay 250000))

queueSleepSharded :: P () ()
queueSleepSharded = Shard queueSleep

runP :: P a b -> [a] -> IO [b]
runP p xs0 = do
  xs <- newTBQueueIO qUEUE_SIZE
  pid <- forkIO $ mapM_ (atomically . writeTBQueue xs) xs0
  ys <- deploy p xs
  replicateM (length xs0) (atomically (readTBQueue ys))
    `finally` killThread pid

runQueueSleepSeq :: IO ()
runQueueSleepSeq = void (runP queueSleepSeq (replicate 5 ()))

runQueueSleep :: IO ()
runQueueSleep = void (runP queueSleep (replicate 5 ()))

runQueueSleepSharded :: IO ()
runQueueSleepSharded = void (runP queueSleepSharded (replicate 5 ()))

copyP :: P () ()
copyP =
  Id :&&& Id :&&& Id :&&& Id :&&& Id
  :>>> Map (const ())

copy10P :: P () ()
copy10P =
  Id :&&& Id :&&& Id :&&& Id :&&& Id :&&&
  Id :&&& Id :&&& Id :&&& Id :&&& Id
  :>>> Map (const ())

copyPSharded :: P () ()
copyPSharded = Shard copyP

noCopyP :: P () ()
noCopyP = Map (const ())

runQueueCopying :: Int -> IO ()
runQueueCopying n = void (runP copyP (replicate n ()))

runQueueCopyingSharded :: Int -> IO ()
runQueueCopyingSharded n = void (runP copyPSharded (replicate n ()))

runQueueNoCopying :: Int -> IO ()
runQueueNoCopying n = void (runP noCopyP (replicate n ()))

runQueueCopying10 :: Int -> IO ()
runQueueCopying10 n = void (runP copy10P (replicate n ()))
