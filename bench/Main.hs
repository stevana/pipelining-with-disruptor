module Main where

import Test.Tasty.Bench

------------------------------------------------------------------------

main :: IO ()
main = defaultMain
  [ bgroup "Twin prime factorisation"
    [ bench "Model"     $ nf   bench_modelTwinPrime []
    , bench "TQueue"    $ nfIO bench_tQueue
    , bench "Disruptor" $ nfIO bench_disruptor
    ]
  ]

bench_modelTwinPrime :: [Int] -> [[Int]]
bench_modelTwinPrime _ = []

bench_tQueue :: IO ()
bench_tQueue = return ()

bench_disruptor :: IO ()
bench_disruptor = return ()
