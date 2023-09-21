{-# LANGUAGE OverloadedStrings #-}

module LibMain.Copying where

import Control.Monad
import System.Environment

import Pipeline
import QueueDeployment (runTBQueueCopying, runTBQueueCopyingSharded)

------------------------------------------------------------------------

iTERATIONS :: Int
iTERATIONS = 4000000

------------------------------------------------------------------------

copyP :: P (Input ()) (Output ())
copyP =
  Identity :&&& Identity :&&& Identity :&&& Identity :&&& Identity
  :>>> Transform "void" (const (Output ()))

copyPSharded :: P (Input ()) (Sharded (Output ()))
copyPSharded = Shard copyP

runDisruptorCopying :: EnableMetrics -> IO ()
runDisruptorCopying enableMetrics =
  void (runPList copyP enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorCopying #-}

runDisruptorCopyingSharded :: EnableMetrics -> IO ()
runDisruptorCopyingSharded enableMetrics =
  void (runPListSharded copyPSharded enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorCopyingSharded #-}


main :: IO ()
main = do
  args <- getArgs
  case args of
    "--tbqueue-no-sharding" : _ -> runTBQueueCopying iTERATIONS
    "--tbqueue" : _             -> runTBQueueCopyingSharded iTERATIONS
    "--no-sharding" : _         -> runDisruptorCopying False
    _otherwise                  -> runDisruptorCopyingSharded False
