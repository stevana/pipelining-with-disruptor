{-# LANGUAGE OverloadedStrings #-}

module LibMain.UpperCase where

import Pipeline
import Data.Char (toUpper)

------------------------------------------------------------------------

upperCase :: P (Input String) (Sharded (Output String))
upperCase = Shard (transform "upperCase" NoOutput (Output . map toUpper))

main :: IO ()
main = runFlow (StdInOutSharded upperCase)
