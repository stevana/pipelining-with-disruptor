{-# LANGUAGE StrictData #-}

module Counter where

import Data.IORef

import Disruptor (SequenceNumber)

------------------------------------------------------------------------

data Counter = Counter [IORef SequenceNumber]

makeCounter :: IORef SequenceNumber -> Counter
makeCounter r = Counter [r]
{-# INLINE makeCounter #-}

readCounter :: Counter -> IO SequenceNumber
readCounter (Counter rs) = minimum <$> traverse readIORef rs
{-# INLINE readCounter #-}

writeCounter :: Counter -> SequenceNumber -> IO ()
writeCounter (Counter rs) i = mapM_ (flip writeIORef i) rs
{-# INLINE writeCounter #-}

combineCounters :: Counter -> Counter -> Counter
combineCounters (Counter rs) (Counter rs') = Counter (rs ++ rs')
{-# INLINE combineCounters #-}
