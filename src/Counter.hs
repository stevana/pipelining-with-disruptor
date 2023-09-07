module Counter where

import Data.IORef

import Disruptor (SequenceNumber)

------------------------------------------------------------------------

data Counter = Counter [IORef SequenceNumber]

makeCounter :: IORef SequenceNumber -> Counter
makeCounter r = Counter [r]

readCounter :: Counter -> IO SequenceNumber
readCounter (Counter rs) = minimum <$> traverse readIORef rs

writeCounter :: Counter -> SequenceNumber -> IO ()
writeCounter (Counter rs) i = mapM_ (flip writeIORef i) rs

combineCounters :: Counter -> Counter -> Counter
combineCounters (Counter rs) (Counter rs') = Counter (rs ++ rs')
