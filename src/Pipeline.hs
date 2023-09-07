{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Pipeline where

import Control.Category (Category)
import qualified Control.Category as Cat
import Control.Concurrent
import Control.Exception
import Control.Monad
import qualified Data.ByteString as BS
import Data.Char
import Data.Coerce
import Data.IORef
import Data.List (intersperse, sort)
import Data.String
import Data.Time
import System.Directory
import System.FilePath ((</>), takeExtension)
import System.IO
import System.Process

import CRC32
import Disruptor (RingBuffer, SequenceNumber)
import qualified Disruptor

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

------------------------------------------------------------------------

class HasRB a where
  data RB a :: *
  new           :: Label -> Int -> IO (RB a)
  cursor        :: RB a -> IORef SequenceNumber
  label         :: RB a -> [Label]
  tryClaim      :: RB a -> IO (Maybe SequenceNumber)
  tryClaimBatch :: RB a -> Int -> IO (Maybe SequenceNumber)
  write         :: RB a -> SequenceNumber -> a -> IO ()
  commit        :: RB a -> SequenceNumber -> IO ()
  commitBatch   :: RB a -> SequenceNumber -> SequenceNumber -> IO ()
  waitFor       :: RB a -> SequenceNumber -> IO SequenceNumber
  readCursor    :: RB a -> IO SequenceNumber
  tryRead       :: RB a -> SequenceNumber -> IO a
  addConsumer   :: RB a -> IO Counter
  toList        :: RB a -> IO [a]

claim :: HasRB a => RB a -> Int -> IO SequenceNumber
claim rb millis = do
  mi <- tryClaim rb
  case mi of
    Nothing -> threadDelay millis >> claim rb millis
    Just i  -> return i

claimBatch :: HasRB a => RB a -> Int -> Int -> IO SequenceNumber
claimBatch rb n millis = do
  mi <- tryClaimBatch rb n
  case mi of
    Nothing -> threadDelay millis >> claimBatch rb n millis
    Just i  -> return i

instance HasRB (Input a) where
  data RB (Input a)             = RBInput [Label] (RingBuffer (Input a))
  new l n                       = RBInput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBInput _l rb) = Disruptor.rbCursor rb
  label         (RBInput l _rb) = l
  tryClaim      (RBInput _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBInput _l rb) = Disruptor.tryClaimBatch rb
  write         (RBInput _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBInput _l rb) = Disruptor.publish rb
  commitBatch   (RBInput _l rb) = Disruptor.publishBatch rb
  waitFor       (RBInput _l rb) = Disruptor.waitFor rb
  readCursor    (RBInput _l rb) = Disruptor.readCursor rb
  tryRead       (RBInput _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBInput _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBInput _l rb) = Disruptor.toList rb

instance HasRB (Output a) where
  data RB (Output a)          = RBOutput [Label] (RingBuffer (Output a))
  new l n                     = RBOutput [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBOutput _l rb) = Disruptor.rbCursor rb
  label         (RBOutput  l _rb) = l
  tryClaim      (RBOutput _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBOutput _l rb) = Disruptor.tryClaimBatch rb
  write         (RBOutput _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBOutput _l rb) = Disruptor.publish rb
  commitBatch   (RBOutput _l rb) = Disruptor.publishBatch rb
  waitFor       (RBOutput _l rb) = Disruptor.waitFor rb
  readCursor    (RBOutput _l rb) = Disruptor.readCursor rb
  tryRead       (RBOutput _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBOutput _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBOutput _l rb) = Disruptor.toList rb

instance HasRB String where
  data RB String           = RB [Label] (RingBuffer String)
  new l n                  = RB [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RB _l rb) = Disruptor.rbCursor rb
  label         (RB  l _rb) = l
  tryClaim      (RB _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RB _l rb) = Disruptor.tryClaimBatch rb
  write         (RB _l rb) = Disruptor.writeRingBuffer rb
  commit        (RB _l rb) = Disruptor.publish rb
  commitBatch   (RB _l rb) = Disruptor.publishBatch rb
  waitFor       (RB _l rb) = Disruptor.waitFor rb
  readCursor    (RB _l rb) = Disruptor.readCursor rb
  tryRead       (RB _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RB _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RB _l rb) = Disruptor.toList rb

instance (HasRB a, HasRB b) => HasRB (a, b) where
  data RB (a, b) = RBPair [Label] (RB a) (RB b)
  new l n = RBPair [l] <$> new l n <*> new l n
  cursor        (RBPair _l xs ys) = undefined
  label         (RBPair  l _xs _ys) = l
  tryClaim rb = undefined
  tryClaimBatch rb = undefined
  addConsumer (RBPair _l xs ys) = do
    c <- addConsumer xs
    d <- addConsumer ys
    return (combineCounters c d)
  waitFor (RBPair _l xs ys) i = do
    hi <- waitFor xs i
    hj <- waitFor ys i
    return (min hi hj)
  tryRead (RBPair _l xs ys) i = do
    x <- tryRead xs i
    y <- tryRead ys i
    return (x, y)
  write = undefined
  commit = undefined
  commitBatch = undefined
  readCursor = undefined
  toList (RBPair _l xs ys) = do
    xs' <- toList xs
    ys' <- toList ys
    return (zip xs' ys')

instance  HasRB (Either a b) where
  data RB (Either a b)        = RBEither [Label] (RingBuffer (Either a b))
  new l n                     = RBEither [l] <$> Disruptor.newRingBuffer_ n
  cursor        (RBEither _l rb) = Disruptor.rbCursor rb
  label         (RBEither  l _rb) = l
  tryClaim      (RBEither _l rb) = Disruptor.tryClaim rb
  tryClaimBatch (RBEither _l rb) = Disruptor.tryClaimBatch rb
  write         (RBEither _l rb) = Disruptor.writeRingBuffer rb
  commit        (RBEither _l rb) = Disruptor.publish rb
  commitBatch   (RBEither _l rb) = Disruptor.publishBatch rb
  waitFor       (RBEither _l rb) = Disruptor.waitFor rb
  readCursor    (RBEither _l rb) = Disruptor.readCursor rb
  tryRead       (RBEither _l rb) = Disruptor.readRingBuffer rb
  addConsumer   (RBEither _l rb) = makeCounter <$> Disruptor.addGatingSequence rb
  toList        (RBEither _l rb) = Disruptor.toList rb
  {-
  data RB (Either a b) = RBEither (RB a) (RB b)
  new n = RBEither <$> new n <*> new n
  tryClaim (RBEither xs ys) (Left x) = tryClaim xs x
  tryClaim (RBEither xs ys) (Right y) = tryClaim ys y
  addConsumer (RBEither xs ys) = do
    c <- addConsumer xs
    d <- addConsumer ys
    return (Disruptor.combineCounters c d)
  waitFor (RBEither xs ys) i = undefined
  write (RBEither xs ys) i (Left  x) = write xs i x
  write (RBEither xs ys) i (Right y) = write ys i y
  commit (RBEither xs ys) (Left x) = commit xs x
  commit (RBEither xs ys) (Right y) = commit ys y
  tryRead (RBEither xs ys) i = undefined
-}

newtype Label = Label String
  deriving (Eq, Show, IsString, Semigroup, Monoid)

infixr 1 :>>>
infixr 3 :&&&
infixr 3 :***
infixr 2 :+++
infixr 2 :|||

-- XXX: Can't have arrow instance because of constraints... Can we get around
-- this using the constrained/restricted monad trick?
data P a b where
  Identity :: P a a
  (:>>>) :: (HasRB b, Show b) => P a b -> P b c -> P a c
  (:***) :: (HasRB a, HasRB b, HasRB c, HasRB d, Show a, Show b, Show c, Show d) => P a b -> P c d -> P (a, c) (b, d)
  (:&&&) :: (HasRB b, HasRB c, Show b, Show c) => P a b -> P a c -> P a (b, c)

  (:+++) :: (HasRB a, HasRB b, HasRB c, HasRB d) => P a c -> P b d -> P (Either a b) (Either c d)
  (:|||) :: (HasRB a, HasRB b) => P a c -> P b c -> P (Either a b) c

  Transform :: Label -> (a -> b) -> P a b
  Fold :: Label -> (a -> s -> (s, b)) -> s -> P a b

  Fork :: P a b -> P a (b, b)

  -- Distr   :: P (Either a b, c) (Either (a, c) (b, c))


rB_SIZE :: Int
rB_SIZE = 8

data Node
  = forall a. (HasRB a, Show a) => RingBufferNode Label (RB a)
  | WorkerNode Label Counter
  | SourceOrSinkNode Label
  | ProducerNode Label

data Edge = Consumes Label Label | Produces Label Label

data Graph = Graph
  { gNodes :: IORef [Node]
  , gEdges :: IORef [Edge]
  }

newGraph :: IO Graph
newGraph = Graph <$> newIORef [] <*> newIORef []

addWorkerNode :: Graph -> Label -> Counter -> IO ()
addWorkerNode g l c = modifyIORef' (gNodes g) (WorkerNode l c :)

addSourceOrSinkNode :: Graph -> Label -> IO ()
addSourceOrSinkNode g l = modifyIORef' (gNodes g) (SourceOrSinkNode l :)

addProducerNode :: Graph -> Label -> IO ()
addProducerNode g l = modifyIORef' (gNodes g) (ProducerNode l :)

addRingBufferNode :: (HasRB a, Show a) => Graph -> Label -> RB a -> IO ()
addRingBufferNode g l r = modifyIORef' (gNodes g) (RingBufferNode l r :)

addEdge :: Graph -> Edge -> IO ()
addEdge g e = modifyIORef' (gEdges g) (e :)

addProducers :: Graph -> Label -> [Label] -> IO ()
addProducers g src dsts = mapM_ (\dst -> modifyIORef' (gEdges g) (Produces src dst :)) dsts

addConsumers :: Graph -> [Label] -> Label -> IO ()
addConsumers g dsts src = mapM_ (\dst -> modifyIORef' (gEdges g) (Consumes src dst :)) dsts

drawGraph :: Graph -> FilePath -> IO ()
drawGraph g fp = withFile fp WriteMode $ \h -> do
  nodes <- reverse <$> readIORef (gNodes g)
  edges <- reverse <$> readIORef (gEdges g)
  hPutStrLn h "digraph g {"

  forM_ nodes $ \node ->
    case node of
      RingBufferNode l r -> do
        i <- readIORef (cursor r)
        xs <- toList r
        let s = concat (intersperse " | " (map show xs))
        hPutStrLn h ("  " ++ coerce l ++ " [shape=Mrecord label=\"<lbl> " ++ coerce l ++
                     " | {" ++ s ++ "} | <seq> " ++ show i ++ "\"]")
      WorkerNode l c -> do
        i <- readCounter c
        hPutStrLn h ("  " ++ coerce l ++ " [shape=record label=\"<lbl> " ++ coerce l ++ " | <seq> " ++ show i ++ "\"]")
      ProducerNode l ->
        hPutStrLn h ("  " ++ coerce l ++ " [shape=record label=\"<lbl> " ++ coerce l ++ "\"]")
      SourceOrSinkNode l ->
        hPutStrLn h ("  " ++ coerce l ++ " [shape=none]")

  hPutStrLn h ""

  let sourceOrSink = foldMap (\node -> case node of
                                         SourceOrSinkNode l -> [l]
                                         _otherwise -> []) nodes
  forM_ edges $ \edge ->
    case edge of
      Produces from to ->
        hPutStrLn h ("  " ++ coerce from ++ " -> " ++ coerce to ++ if to `elem` sourceOrSink then "" else ":lbl")
      Consumes from to -> hPutStrLn h ("  " ++ coerce from ++ " -> " ++ coerce to ++ ":seq [style=dashed]")

  hPutStrLn h "}"
  hFlush h

-- Measure:
--  * saturation (queue lengths)
--  * utilisation (opposite to idle time)
--  * latency (time item is waiting in queue)
--  * service time (time to handle item)
--  * throughput (items per unit of time)
-- https://github.com/nozcat/hash-pipeline/blob/main/src/main.rs#L117

runDot :: FilePath -> IO ()
runDot dir = do
  fps <- listDirectory dir
  let dots = map (dir </>) (filter (\fp -> takeExtension fp == ".dot") fps)
  go (sort dots) Nothing Nothing
  where
    go []        _prevSz _prevHash = return ()
    go (fp : fps) prevSz  prevHash =
      withFile fp ReadMode $ \h -> do
        sz <- Just <$> hFileSize h
        bs <- BS.hGetContents h
        let hash = Just (crc32 bs)
        if sz == prevSz && hash == prevHash
        then go fps sz hash
        else do
          callProcess "dot" ["-Tsvg", "-O", dir </> fp]
          go fps sz hash

runFeh :: FilePath -> IO ()
runFeh dir = do
  fps <- listDirectory dir
  let svgs = map (dir </>) (filter (\fp -> takeExtension fp == ".svg") fps)
  callProcess "feh" (sort svgs)

deploy :: (HasRB a, HasRB b, Show a, Show b) => P a b -> Graph -> RB a -> IO (RB b)
deploy Identity g xs = return xs
deploy (p :>>> q) g xs = deploy p g xs >>= deploy q g
deploy (p :*** q) g (RBPair l xs ys) = do
  xs' <- deploy p g xs
  ys' <- deploy q g ys
  return (RBPair l xs' ys')
deploy (p :&&& q) g xs = do
  ys <- deploy p g xs
  zs <- deploy q g xs
  return (RBPair (label ys ++ label zs) ys zs)
deploy (Transform l f) g xs = do
  ys <- new (l <> "_RB") rB_SIZE
  addRingBufferNode g (l <> "_RB") ys
  addConsumers g (label xs) l
  c <- addConsumer xs
  addWorkerNode g l c
  addProducers g l (label ys)
  pid <- forkIO $ forever $ do
    consumed <- readCounter c
    produced <- waitFor xs consumed
    Disruptor.iter consumed produced $ \i -> do
      x <- tryRead xs i
      -- threadDelay 1100000 -- XXX: For debugging
      write ys i (f x)
    commitBatch ys consumed produced
    writeCounter c produced
  return ys
deploy (Fold l f s0) g xs = do
  ys <- new (l <> "_RB") rB_SIZE
  addRingBufferNode g (l <> "_RB") ys
  addConsumers g (label xs) l
  addProducers g l (label ys)
  c <- addConsumer xs
  addWorkerNode g l c
  let go s0 = do
        consumed <- readCounter c
        produced <- waitFor xs consumed
        s' <- Disruptor.fold consumed produced s0 $ \i s -> do
          x <- tryRead xs i
          let (s', y) = f x s
          -- threadDelay 1050000 -- XXX: For debugging
          write ys i y
          return s'
        commitBatch ys consumed produced
        writeCounter c produced
        go s'
  pid <- forkIO (go s0)
  return ys

  {-
deploy (p :+++ q) (RBEither xs ys) = do
  xs' <- deploy p xs
  ys' <- deploy q ys
  return (RBEither xs' ys')
deploy (p :||| q) (RBEither xs ys) = do
  zs  <- deploy p xs
  zs' <- deploy q ys
  deploy (Transform (either id id)) (RBEither zs zs')
-}

data Input  a = Input  !a | EndOfStream
data Output b = Output !b | NoOutput

instance Show a => Show (Input a) where
  show (Input x)   = escape $ show x
  show EndOfStream = "EndOfStream"

instance Show b => Show (Output b) where
  show (Output y) = escape $ show y
  show NoOutput   = "NoOutput"

escape :: String -> String
escape []         = []
escape ('\r' : cs) = '\\' : '\r' : escape cs
escape ('\\' : '"' : cs) = '\\' : '"' : escape cs
escape ('"' : cs) = '\\' : '"' : escape cs
escape ('-' : cs) = '\\' : '-' : escape cs
escape ('[' : cs) = '\\' : '[' : escape cs
escape (']' : cs) = '\\' : ']' : escape cs
escape (c   : cs) = c : escape cs

data Flow
  = StdInOut (P (Input String) (Output String))
  | forall a b. Show b => List [a] (P (Input a) (Output b))

runFlow :: Flow -> IO ()
runFlow (StdInOut p) = do

  xs <- new "source_RB" rB_SIZE
  g  <- newGraph
  addSourceOrSinkNode g "stdin"
  addProducerNode g "source"
  addProducers g "stdin" ["source"]
  addProducers g "source" ["source_RB"]
  addRingBufferNode g "source_RB" xs
  ys <- deploy p g xs
  stop <- newIORef (-1)

  t0 <- getCurrentTime
  let dateFormat = "%F_%T%Q" -- YYYY-MM-DD_HH:MM:SS.PICOS
  let dir = "/tmp/wc-metrics-" ++ formatTime defaultTimeLocale dateFormat t0
  createDirectoryIfMissing True dir
  let metrics = forever $ do
        t <- getCurrentTime
        drawGraph g (dir </> "wc-" ++ formatTime defaultTimeLocale dateFormat t ++ ".dot")
        threadDelay 1000 -- 0.001s
  pidMetrics <- forkIO metrics

  let source = do
        es <- fmap Input getLine `catch` (\(_e :: IOError) -> return EndOfStream)
        i <- claim xs 1
        threadDelay 100000 -- XXX: For debugging
        write xs i es
        commit xs i
        case es of
          EndOfStream -> atomicWriteIORef stop i
          Input _     -> source
  pidSource <- forkIO source

  c <- addConsumer ys
  addWorkerNode g "sink" c
  addConsumers g (label ys) "sink"
  addSourceOrSinkNode g "stdout"
  addProducers g "sink" ["stdout"]
  let sink = do
        i <- readIORef stop
        consumed <- readCounter c
        produced <- readCursor ys
        -- NOTE: `waitFor` is inlined here, so that we can stop.
        if i /= -1 && consumed == i
        then return ()
        else do
          Disruptor.iter consumed produced $ \i -> do
            ms <- tryRead ys i
            case ms of
              NoOutput -> return ()
              Output s -> putStrLn s
          -- XXX: is it worth avoiding this write when produced == consumed?
          writeCounter c produced
          threadDelay 1 -- NOTE: Without this sleep we get into an infinite
                        -- loop... Not sure why.
          sink
  sink `finally` do
    mapM_ killThread [pidSource, pidMetrics]
    runDot dir
    runFeh dir
-- runFlow (List xs0 p) = do
--   -- XXX: Max size of RB?
--   let n = length xs0
--   -- XXX: round up to nearest power of two?
--   xs <- new "List source" n
--   g  <- newGraph
--   pidSource <- forkIO $ do
--     n' <- claimBatch xs n 100
--     assertIO (n == coerce n')
--     mapM_ (\(i, x) -> write xs i (Input x)) (zip [0..n'] xs0)
--     commitBatch xs 0 n'
--
--   ys <- deploy p g xs
--   c <- addConsumer ys
--   let sink = do
--         consumed <- readCounter c
--         if consumed == coerce n then return ()
--         else do
--           produced <- waitFor ys consumed
--           Disruptor.iter consumed produced $ \i -> do
--             my <- tryRead ys i
--             case my of
--               NoOutput -> return ()
--               Output y -> print y
--             writeCounter c produced
--             sink
--   sink `finally` killThread pidSource

transform :: Label -> Output b -> (a -> Output b) -> P (Input a) (Output b)
transform l y f = Transform l (\i -> case i of
                                       Input x     -> f x
                                       EndOfStream -> y)

fold :: Label -> s -> (s -> Output b) -> (a -> s -> (s, Output b)) -> P (Input a) (Output b)
fold l s0 e f = Fold l (\i s -> case i of
                                  Input x     -> f x s
                                  EndOfStream -> (s, e s))
                   s0

main :: IO ()
main = runFlow (StdInOut wc)
  -- runFlow (List ["apa bepa", "cepa", "depa"] wc)
  where
    upperCase = transform "upperCase" NoOutput (Output . map toUpper)

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

------------------------------------------------------------------------

assertIO :: Bool -> IO ()
assertIO !b = assert b (return ())
