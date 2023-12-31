{-# LANGUAGE NumericUnderscores #-}

module Main where

import Control.DeepSeq (force)
import Control.Exception (evaluate)
import Test.Tasty.Bench

import Pipeline
import LibMain.Sleep hiding (iTERATIONS)
import qualified ModelIO
import QueueDeployment

------------------------------------------------------------------------

iTERATIONS :: Int
iTERATIONS = 5

main :: IO ()
main = defaultMain
  [ env (evaluate (force (replicate iTERATIONS ()))) $ \xs ->
    bgroup "Sleep"
      [ bench "Model"            $ nfIO $ bench_modelSleep            xs
      , bench "TBQueue"          $ nfIO $ bench_queueSleep            xs
      , bench "TBQueueSharded"   $ nfIO $ bench_queueSleepSharded     xs
      , bench "Disruptor"        $ nfIO $ bench_disruptorSleep        xs
      , bench "DisruptorSharded" $ nfIO $ bench_disruptorSleepSharded xs
      ]
  ]

bench_modelSleep :: [()] -> IO ()
bench_modelSleep xs = do
  ys <- ModelIO.model ModelIO.modelSleep xs
  _ <- evaluate (force ys)
  return ()

bench_queueSleep :: [()] -> IO [()]
bench_queueSleep = runP queueSleep

bench_queueSleepSharded :: [()] -> IO [()]
bench_queueSleepSharded = runP queueSleepSharded

bench_disruptorSleep :: [()] -> IO ()
bench_disruptorSleep = runPList sleepP False

bench_disruptorSleepSharded :: [()] -> IO ()
bench_disruptorSleepSharded = runPListSharded sleepPSharded False
