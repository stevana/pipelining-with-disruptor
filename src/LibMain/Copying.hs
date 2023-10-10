{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module LibMain.Copying where

import Control.Monad
import System.Environment

import Pipeline
import QueueDeployment
       ( runQueueCopying
       , runQueueCopying10
       , runQueueCopyingSharded
       , runQueueNoCopying
       )

------------------------------------------------------------------------

iTERATIONS :: Int
iTERATIONS = 5_000_000

------------------------------------------------------------------------

copyP :: P (Input ()) (Output ())
copyP =
  Identity :&&& Identity :&&& Identity :&&& Identity :&&& Identity
  :>>> Transform "void" (const (Output ()))

copy10P :: P (Input ()) (Output ())
copy10P =
  Identity :&&& Identity :&&& Identity :&&& Identity :&&& Identity :&&&
  Identity :&&& Identity :&&& Identity :&&& Identity :&&& Identity
  :>>> Transform "void" (const (Output ()))

copyPSharded :: P (Input ()) (Sharded (Output ()))
copyPSharded = Shard copyP

noCopyP :: P (Input ()) (Output ())
noCopyP = Transform "void" (const (Output ()))

runDisruptorCopying :: EnableMetrics -> IO ()
runDisruptorCopying enableMetrics =
  void (runPList copyP enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorCopying #-}

runDisruptorCopyingSharded :: EnableMetrics -> IO ()
runDisruptorCopyingSharded enableMetrics =
  void (runPListSharded copyPSharded enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorCopyingSharded #-}

runDisruptorNoCopying :: EnableMetrics -> IO ()
runDisruptorNoCopying enableMetrics =
  void (runPList noCopyP enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorNoCopying #-}

runDisruptorCopying10 :: EnableMetrics -> IO ()
runDisruptorCopying10 enableMetrics =
  void (runPList copy10P enableMetrics (replicate iTERATIONS ()))
{-# INLINE runDisruptorCopying10 #-}


main :: IO ()
main = do
  args <- getArgs
  case args of
    "--tbqueue-no-copying" : _  -> runQueueNoCopying iTERATIONS
    "--tbqueue-no-sharding" : _ -> runQueueCopying iTERATIONS
    "--tbqueue-copy10" : _      -> runQueueCopying10 iTERATIONS
    "--tbqueue" : _             -> runQueueCopyingSharded iTERATIONS
    "--no-copying" : _          -> runDisruptorNoCopying False
    "--no-sharding" : _         -> runDisruptorCopying False
    "--copy10" : _              -> runDisruptorCopying10 False
    _otherwise                  -> runDisruptorCopyingSharded False
