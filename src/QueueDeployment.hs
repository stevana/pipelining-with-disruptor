{-# LANGUAGE GADTs #-}

module QueueDeployment where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Test.QuickCheck.Monadic

import Model

------------------------------------------------------------------------

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
deploy _ _ = error "not implemented"

example' :: [Int] -> IO [(Int, Bool)]
example' xs0 = do
  xs <- newTQueueIO
  mapM_ (atomically . writeTQueue xs) xs0
  ys <- deploy (Id :&&& Map even) xs
  replicateM (length xs0) (atomically (readTQueue ys))

prop_commute :: Eq b => P a b -> [a] -> PropertyM IO ()
prop_commute p xs = do
  ys <- run $ do
    qxs <- newTQueueIO
    mapM_ (atomically . writeTQueue qxs) xs
    qys <- deploy p qxs
    replicateM (length xs) (atomically (readTQueue qys))
  assert (model p xs == ys)
