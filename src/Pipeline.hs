{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}

module Pipeline (module Pipeline, Input(..), Output(..), module Sharding) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Coerce
import Data.Maybe
import Data.IORef
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Time
import System.Directory (createDirectoryIfMissing)
import System.FilePath ((</>))

import Counter
import Disruptor (SequenceNumber, WaitStrategy(..))
import qualified Disruptor
import RingBufferClass
import Sharding
import Visualise

------------------------------------------------------------------------

rB_SIZE :: Int
rB_SIZE = 2^(16 :: Int)

rB_BATCH_SIZE :: Int
rB_BATCH_SIZE = 128

rB_WAIT_STRATEGY :: WaitStrategy
rB_WAIT_STRATEGY = MVar -- Spin 1

rB_BACK_PRESSURE :: IO ()
rB_BACK_PRESSURE = threadDelay 100

dATE_FORMAT :: String
dATE_FORMAT = "%F_%T%Q" -- YYYY-MM-DD_HH:MM:SS.PICOS

------------------------------------------------------------------------

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
  TransformM :: Label -> (a -> IO b) -> P a b
  Fold :: Label -> (a -> s -> (s, b)) -> s -> P a b

  Shard :: (HasRB b, Show b) => P a b -> P a (Sharded b)
  Fork  :: P a b -> P a (b, b)
  -- Distr   :: P (Either a b, c) (Either (a, c) (b, c))

  -- Decompose :: (a -> [b]) -> P a b
  -- Recompose :: (a -> [a] -> Maybe b) -> P a b
  -- Tee       :: P a b -> P a a
  -- DeadEnd   :: P a ()

data DeployEnv = DeployEnv
  { deGraph     :: Graph
  , deSharding  :: Sharding
  , deThreadIds :: IORef [ThreadId]
  , deCPUMap    :: Map Label Int
  }

saveThreadId :: DeployEnv -> ThreadId -> IO ()
saveThreadId de tid = modifyIORef' (deThreadIds de) (tid :)

loadThreadIds :: DeployEnv -> IO [ThreadId]
loadThreadIds = readIORef . deThreadIds

deploy :: (HasRB a, HasRB b, Show a, Show b) => P a b -> DeployEnv -> RB a -> IO (RB b)
deploy Identity   _e xs = return xs
deploy (p :>>> q) e  xs = deploy p e xs >>= deploy q e
deploy (p :*** q) e (RBPair l xs ys) = do
  xs' <- deploy p e xs
  ys' <- deploy q e ys
  return (RBPair l xs' ys')
deploy (p :&&& q) e xs = do
  ys <- deploy p e xs
  zs <- deploy q e xs
  return (RBPair (label ys ++ label zs) ys zs)
deploy (Transform l f) e xs = deploy (TransformM l (return . f)) e xs
deploy (TransformM l f) e xs = do
  ys <- new (l <> "_RB") rB_SIZE rB_WAIT_STRATEGY
  addRingBufferNode (deGraph e) (l <> "_RB") (deSharding e) ys
  addConsumers (deGraph e) (label xs) l
  c <- addConsumer xs
  addWorkerNode (deGraph e) l c
  addProducers (deGraph e) l (label ys)
  let fork = case Map.lookup l (deCPUMap e) of
               Nothing -> forkIO
               Just n  -> forkOn n
  pid <- fork $ forever $ do
    consumed <- readCounter c
    -- putStrLn (unLabel l ++ ", waitFor: " ++ show consumed)
    produced <- waitFor xs consumed
    Disruptor.iter consumed produced $ \i ->
      when (partition i (deSharding e)) $ do
        x <- tryRead xs i
        -- XXX: For debugging:
        -- delay <- randomRIO (50000, 300000)
        -- threadDelay delay
        -- putStrLn (unLabel l ++ ", i: " ++ show i)
        -- start <- getCurrentTime
        y <- f x
        -- end <- getCurrentTime
        -- putStrLn (unLabel l ++ ": executing task: " ++ show i ++ " ... finished in: " ++ show (diffUTCTime end start))
        write ys i y
        commit ys i

    -- NOTE: Committing the whole batch at once, slow things down for the
    -- downstream (at least on small workloads).
    -- commitBatch ys consumed produced
    writeCounter c produced
  -- (capa, _pinned) <- threadCapability pid
  -- putStrLn (unLabel l ++ " is running on capability: " ++ show capa)
  saveThreadId e pid
  return ys
deploy (Fold l f s00) e xs = do
  ys <- new (l <> "_RB") rB_SIZE rB_WAIT_STRATEGY
  let g  = deGraph e
  addRingBufferNode g (l <> "_RB") (deSharding e) ys
  addConsumers g (label xs) l
  addProducers g l (label ys)
  c <- addConsumer xs
  addWorkerNode g l c
  let go s0 = do
        consumed <- readCounter c
        produced <- waitFor xs consumed
        s' <- Disruptor.fold consumed produced s0 $ \i s -> do
          if partition i (deSharding e)
          then do
            x <- tryRead xs i
            let (s', y) = f x s
            -- XXX: For debugging:
            -- delay <- randomRIO (50000, 3000000)
            -- threadDelay delay
            write ys i y
            return s'
          else return s
        commitBatch ys consumed produced
        writeCounter c produced
        go s'
  pid <- forkIO (go s00)
  saveThreadId e pid
  return ys
deploy (_ :+++ _) _e _ = undefined
deploy (_ :||| _) _e _ = undefined
deploy (Fork _)   _e _ = undefined
deploy (Shard p) e xs  = do
  let p' = appendPrimeToLabels p
  let (s1, s2) = addShard (deSharding e)
  ys1 <- deploy p' (e { deSharding = s1 }) xs
  ys2 <- deploy p' (e { deSharding = s2 }) xs
  return (RBShard (label ys1 ++ label ys2) s1 s2 ys1 ys2)
{-# INLINE deploy #-}

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

appendPrimeToLabels :: P a b -> P a b
appendPrimeToLabels = id -- XXX
{-# INLINE appendPrimeToLabels #-}

data Flow
  = StdInOut (P (Input String) (Output String))
  | StdInOutSharded (P (Input String) (Sharded (Output String)))

data SourceSetup a = SourceSetup (RB (Input a)) Graph ThreadId (IORef SequenceNumber)

setupSource :: (HasRB a, Show a) => Label -> (RB (Input a) -> IORef SequenceNumber -> IO ())
            -> IO (SourceSetup a)
setupSource l src = do
  xs <- new "source_RB" rB_SIZE rB_WAIT_STRATEGY
  g  <- newGraph
  addSourceOrSinkNode g l
  addProducerNode g "source"
  addProducers g l ["source"]
  addProducers g "source" ["source_RB"]
  addRingBufferNode g "source_RB" noSharding xs
  stop <- newIORef (-1)
  pid <- forkIO (src xs stop)
  return (SourceSetup xs g pid stop)
{-# INLINE setupSource #-}

data MetricsSetup = MetricsSetup FilePath (Maybe ThreadId)

-- https://news.ycombinator.com/item?id=37532439
setupMetrics :: EnableMetrics -> Graph -> IORef SequenceNumber -> IO MetricsSetup
setupMetrics True g stop = do
  t0 <- getCurrentTime
  let dir = "/tmp/wc-metrics-" ++ formatTime defaultTimeLocale dATE_FORMAT t0
  createDirectoryIfMissing True dir
  let metrics = do
        t <- getCurrentTime
        drawGraph g (dir </> "wc-" ++ formatTime defaultTimeLocale dATE_FORMAT t ++ ".dot")
        threadDelay 1000 -- 0.001s
        stopping <- readIORef stop
        if stopping == (-1)
        then metrics
        else return ()
  pid <- forkOn 0 metrics
  return (MetricsSetup dir (Just pid))
setupMetrics False _g _stop =
  return (MetricsSetup "/nonexistent" Nothing)

newtype SinkSetup = SinkSetup (IO ())

setupSink :: HasRB a => Label -> RB a -> Graph -> IORef SequenceNumber -> (a -> IO ()) -> IO SinkSetup
setupSink l ys g stop io = do
  c <- addConsumer ys
  addWorkerNode g "sink" c
  addConsumers g (label ys) "sink"
  addSourceOrSinkNode g l
  addProducers g "sink" [l]
  let sink = do
        stopping <- readIORef stop
        consumed <- readCounter c
        -- XXX: remove
        -- when (stopping /= (-1)) $
          -- putStrLn ("stopping: " ++ show stopping ++ ", consumed: " ++ show consumed)
        produced <- readCursor ys
        -- NOTE: `waitFor` is inlined here, so that we can stop.
        if stopping /= -1 && consumed >= stopping
        then return ()
        else do
          Disruptor.iter consumed produced $ \i -> do
            ms <- tryRead ys i
            io ms
          -- XXX: is it worth avoiding this write when produced == consumed?
          writeCounter c produced
          threadDelay 1 -- NOTE: Without this sleep we get into an infinite
                        -- loop... Not sure why.
          sink
  return (SinkSetup sink)
{-# INLINE setupSink #-}

type EnableMetrics = Bool

flowBracket :: (HasRB a, HasRB b, Show a, Show b)
            => P (Input a) b -> Label
            -> (RB (Input a) -> IORef SequenceNumber -> IO ())
            -> Label -> (b -> IO ()) -> EnableMetrics -> IO ()
flowBracket p srcLabel src snkLabel sink enableMetrics = do
  SourceSetup xs g sourcePid stop <- setupSource srcLabel src
  threadIds <- newIORef []
  -- XXX: don't hardcode topology...
  let deployEnv = DeployEnv g noSharding threadIds (Map.fromList [("sleep1", 2), ("sleep2", 3), ("sleep3", 4), ("sleep4", 5)])
  ys <- deploy p deployEnv xs
  MetricsSetup dir metricsPid <- setupMetrics enableMetrics g stop
  SinkSetup runSink <- setupSink snkLabel ys g stop sink

  runSink `finally` do
    workerPids <- loadThreadIds deployEnv
    mapM_ killThread (sourcePid : maybeToList metricsPid ++ workerPids)
    when enableMetrics $ do
      t <- getCurrentTime
      drawGraph g (dir </> "wc-" ++ formatTime defaultTimeLocale dATE_FORMAT t ++ ".dot")
      runDot dir
      runFeh dir
{-# INLINE flowBracket #-}

listSource :: [a] -> IO (IO (Input a))
listSource xs0 = do
  input <- newIORef xs0
  let src = do
        xs <- readIORef input
        case xs of
          []      -> return EndOfStream
          x : xs' -> do
            writeIORef input xs'
            return (Input x)
  return src

runPList :: (HasRB a, Show a, Show b) => P (Input a) (Output b) -> EnableMetrics -> [a] -> IO ()
runPList p enableMetrics xs = do
  src <- listSource xs

  -- output <- newIORef []
  let snk ms = case ms of
                 NoOutput -> return ()
                 Output _y -> return () -- modifyIORef output (y :)
  flowBracket p "list" (batching rB_BATCH_SIZE src) "stdout" snk enableMetrics
  -- readIORef output
{-# INLINE runPList #-}

runPListSharded :: (HasRB a, Show a, Show b) => P (Input a) (Sharded (Output b)) -> EnableMetrics -> [a] -> IO ()
runPListSharded p enableMetrics xs = do
  src <- listSource xs

  -- output <- newIORef []
  let snk (Sharded ms) = case ms of
                           NoOutput -> return ()
                           Output _y -> return () -- modifyIORef' output (y :)
  flowBracket p "list" (batching rB_BATCH_SIZE src) "stdout" snk enableMetrics
  -- readIORef output
{-# INLINE runPListSharded #-}


nonBatching :: IO (Input a) -> RB (Input a) -> IORef SequenceNumber -> IO ()
nonBatching io xs stop = go
  where
    go = do
      mi <- tryClaim xs
      if mi == Disruptor.nothingSN
      then do
        rB_BACK_PRESSURE
        go
      else do
        es <- io
        -- XXX: For debugging:
        -- delay <- randomRIO (5000, 30000)
        -- threadDelay delay
        write xs (coerce mi) es
        commit xs (coerce mi)
        case es of
          EndOfStream -> writeIORef stop (coerce mi)
          Input _     -> go

batching :: Int -> IO (Input a) -> RB (Input a) -> IORef SequenceNumber -> IO ()
batching n io xs stop = go
  where
    go = do
      mhi <- tryClaimBatch xs n
      if mhi == Disruptor.nothingSN
      then do
        rB_BACK_PRESSURE
        go
      else do
        -- XXX: For debugging:
        -- delay <- randomRIO (5000, 30000)
        -- threadDelay delay
        let hi = coerce mhi
            lo = hi - coerce n
        -- putStrLn $ "batching, lo: " ++ show lo ++ ", hi: " ++ show hi
        go' (lo + 1) hi
        commit xs hi
      where
        go' :: SequenceNumber -> SequenceNumber -> IO ()
        go' !lo hi | lo > hi   = go
                   | otherwise = do
                       mx <- io
                       case mx of
                         Input _ -> do
                           write xs lo mx
                           go' (lo + 1) hi
                         EndOfStream -> writeIORef stop lo

runFlow :: Flow -> IO ()
runFlow (StdInOut p) = do
  let src    = fmap Input getLine `catch` (\(_e :: IOError) -> return EndOfStream)
      snk ms = case ms of
                  NoOutput -> return ()
                  Output s -> putStrLn s
  flowBracket p "stdin" (nonBatching src) "stdout" snk True
runFlow (StdInOutSharded p) = do
  let src = fmap Input getLine `catch` (\(_e :: IOError) -> return EndOfStream)
      snk (Sharded ms) = case ms of
                           NoOutput -> return ()
                           Output s -> putStrLn s

  flowBracket p "stdin" (nonBatching src) "stdout" snk True

transform :: Label -> Output b -> (a -> Output b) -> P (Input a) (Output b)
transform l y f = Transform l (\i -> case i of
                                       Input x     -> f x
                                       EndOfStream -> y)

fold :: Label -> s -> (s -> Output b) -> (a -> s -> (s, Output b)) -> P (Input a) (Output b)
fold l s0 e f = Fold l (\i s -> case i of
                                  Input x     -> f x s
                                  EndOfStream -> (s, e s))
                   s0

------------------------------------------------------------------------

assertIO :: Bool -> IO ()
assertIO !b = assert b (return ())

timeIt :: IO () -> IO ()
timeIt io = do
  start <- getCurrentTime
  io
  end <- getCurrentTime
  print (diffUTCTime end start)
