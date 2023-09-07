{-# LANGUAGE OverloadedStrings #-}

module LibMain.WordCount where

import Pipeline

------------------------------------------------------------------------

lineCount, wordCount, charCount :: P (Input String) (Output String)
lineCount = fold "lineCount" 0 (Output . show) (\_str n -> (n + 1,                  NoOutput))
wordCount = fold "wordCount" 0 (Output . show) (\str  n -> (n + length (words str), NoOutput))
charCount = fold "charCount" 0 (Output . show) (\str  n -> (n + length str + 1,     NoOutput))

wc :: P (Input String) (Output String)
wc = (lineCount :&&& wordCount :&&& charCount) :>>> Transform "combine" combine
  where
    combine (ms, (ms', ms'')) =
      case (ms, ms', ms'') of
        (NoOutput, NoOutput, NoOutput)    -> NoOutput
        (Output s, Output s', Output s'') -> Output (s ++ " " ++ s' ++ " " ++ s'')
        _otherwise -> error (show _otherwise)

main :: IO ()
main = runFlow (StdInOut wc)
