{-# LANGUAGE OverloadedStrings #-}

module LibMain.Factor where

import Pipeline

------------------------------------------------------------------------

-- Compute the prime factors for an integer, taken from
-- https://en.wikipedia.org/wiki/Trial_division.
trialDivision :: Int -> [Int]
trialDivision = go [] 2
  where
    go :: [Int] -> Int -> Int -> [Int]
    go a f n | n <= 1    = reverse a
             | otherwise = if n `mod` f == 0
                           then go (f : a) f (n `div` f)
                           else go a (f + 1) n

factor :: P (Input String) (Sharded (Output String))
factor = Shard (transform "factor" endOfStream (Output . show . trialDivision . read))
  where
    endOfStream = NoOutput

main :: IO ()
main = runFlow (StdInOutSharded factor)

-- XXX: Fanout and check for twin primes, i.e. where (p, p+2) are both prime.
-- https://en.wikipedia.org/wiki/List_of_prime_numbers
