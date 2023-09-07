{-# LANGUAGE OverloadedStrings #-}

module LibMain.UpperCase where

import Pipeline
import Data.Char (toUpper)

------------------------------------------------------------------------

upperCase :: P (Input String) (Output String)
upperCase = transform "upperCase" NoOutput (Output . map toUpper)

main :: IO ()
main = runFlow (StdInOut upperCase)
