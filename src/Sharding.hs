{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE StrictData #-}

module Sharding where

import Control.Exception
import Data.Array.MArray
import Data.Bits
import Data.Coerce

import Disruptor

------------------------------------------------------------------------

newtype Sharded a = Sharded a
  deriving Show

data Partition = Partition
  { pIndex :: Int
  , pTotal :: Int
  }
  deriving Show

noPartition :: Partition
noPartition = Partition 0 1

addPartition :: Partition -> (Partition, Partition)
addPartition (Partition i total) =
  ( Partition i (total * 2)
  , Partition (i + total) (total * 2)
  )

partition :: SequenceNumber -> Partition -> Bool
partition i (Partition n total) = coerce i .&. (total - 1) == 0 + n

toListSharded_ :: forall a. RingBuffer a -> Partition -> IO [a]
toListSharded_ rb s = do
  produced <- readCursor rb
  if coerce produced < capacity rb - 1
  then goSmall 0 (coerce produced) []
  else goBig (coerce produced) 1 (coerce (capacity rb)) []
  where
    goSmall :: Int -> Int -> [a] -> IO [a]
    goSmall lo hi acc
      | lo >  hi = return (reverse acc)
      | lo <= hi = do
          if partition (coerce lo) s
          then do
            -- putStrLn $ "toListSharded_, small in partition: " ++ show lo ++ ", " ++ show s
            -- XXX: use unsafeRead?
            !x <- readArray (elements rb) lo
            goSmall (lo + 1) hi (x : acc)
          else do
            -- putStrLn $ "toListSharded_, small not in partition: " ++ show lo
            goSmall (lo + 1) hi acc

    goBig :: Int -> Int -> Int -> [a] -> IO [a]
    goBig produced lo hi acc
      | lo >  hi = return (reverse acc)
      | lo <= hi = do
          -- putStrLn $ "toListSharded_: " ++ show lo
          let ix = Disruptor.index (capacity rb) (coerce (produced + lo))
          if partition (coerce ix) s
          then do
            -- XXX: use unsafeRead?
            !x <- readArray (elements rb) ix
            goBig produced (lo + 1) hi (x : acc)
          else
            goBig produced (lo + 1) hi acc
