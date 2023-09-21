{-# LANGUAGE OverloadedStrings #-}

module LibMain.Sleep where

import Control.Monad
import Control.Concurrent
import System.Environment

import Pipeline

------------------------------------------------------------------------

iTERATIONS :: Int
iTERATIONS = 10000

sLEEP_MICROS :: Int
sLEEP_MICROS = 25

------------------------------------------------------------------------

sleepP :: P (Input ()) (Output ())
sleepP = TransformM "sleep1" (const (Output <$> threadDelay sLEEP_MICROS)) :&&&
         TransformM "sleep2" (const (Output <$> threadDelay sLEEP_MICROS)) :>>>
         TransformM "sleep3" (const (Output <$> threadDelay sLEEP_MICROS)) :>>>
         TransformM "sleep4" (const (Output <$> threadDelay sLEEP_MICROS))

sleepPSharded :: P (Input ()) (Sharded (Output ()))
sleepPSharded = Shard sleepP

runDisruptorSleep :: EnableMetrics -> IO ()
runDisruptorSleep enableMetrics =
  void (runPList sleepP enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorSleep #-}

runDisruptorSleepSharded :: EnableMetrics -> IO ()
runDisruptorSleepSharded enableMetrics =
  void (runPListSharded sleepPSharded enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorSleepSharded #-}

main :: IO ()
main = do
  args <- getArgs
  let sharded       = any ((==) "--sharded") args
      enableMetrics = any (\arg -> arg == "--metrics" ||
                                   arg == "--enable-metrics") args
  if sharded
  then do
    putStrLn "Running sharded sleep on Disruptor pipeline"
    timeIt $ runDisruptorSleepSharded enableMetrics
  else do
    putStrLn "Running sleep on Disruptor pipeline"
    timeIt $ runDisruptorSleep enableMetrics
