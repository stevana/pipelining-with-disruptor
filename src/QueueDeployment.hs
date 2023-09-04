{-# LANGUAGE GADTs #-}

module QueueDeployment where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM

------------------------------------------------------------------------

data P a b where
  Id      :: P a a
  (:>>>)  :: P a b -> P b c -> P a c
  Map     :: (a -> b) -> P a b
  -- (:***)  :: P a c -> P b d -> P (a, b) (c, d)
  (:&&&)  :: P a b -> P a c -> P a (b, c)
  -- (:+++)  :: P a c -> P b d -> P (Either a b) (Either c d)
  -- (:|||)  :: P a c -> P b c -> P (Either a b) c

deploy :: P a b -> TQueue a -> IO (TQueue b)
deploy Id         xs = return xs
deploy (f :>>> g) xs = deploy g =<< deploy f xs
deploy (Map f)    xs = do
  ys <- newTQueueIO
  _pid <- forkIO $ forever $ do
    x <- atomically (readTQueue xs)
    let y = f x
    atomically (writeTQueue ys y)
  return ys
deploy (f :&&& g) xs = do
  xs1 <- newTQueueIO
  xs2 <- newTQueueIO
  _pid <- forkIO $ forever $ do
    x <- atomically (readTQueue xs)
    atomically $ do
      writeTQueue xs1 x
      writeTQueue xs2 x
  ys <- deploy f xs1
  zs <- deploy g xs2
  yzs <- newTQueueIO
  _pid <- forkIO $ forever $ do
    y <- atomically (readTQueue ys)
    z <- atomically (readTQueue zs)
    atomically (writeTQueue yzs (y, z))
  return yzs

example' :: [Int] -> IO [(Int, Bool)]
example' xs0 = do
  xs <- newTQueueIO
  mapM_ (atomically . writeTQueue xs) xs0
  ys <- deploy (Id :&&& Map even) xs
  replicateM (length xs0) (atomically (readTQueue ys))
