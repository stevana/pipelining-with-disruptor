{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Pipeline (module Pipeline, module RingBufferClass) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Time
import System.Directory (createDirectoryIfMissing)
import System.FilePath ((</>))
import System.Random

import Counter
import qualified Disruptor
import RingBufferClass
import Visualise

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
  Fold :: Label -> (a -> s -> (s, b)) -> s -> P a b

  Fork :: P a b -> P a (b, b)
  -- Distr   :: P (Either a b, c) (Either (a, c) (b, c))

rB_SIZE :: Int
rB_SIZE = 8

deploy :: (HasRB a, HasRB b, Show a, Show b) => P a b -> Graph -> RB a -> IO (RB b)
deploy Identity _g  xs = return xs
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
  _pid <- forkIO $ forever $ do
    consumed <- readCounter c
    produced <- waitFor xs consumed
    Disruptor.iter consumed produced $ \i -> do
      x <- tryRead xs i
      -- XXX: For debugging:
      delay <- randomRIO (50000, 3000000)
      threadDelay delay
      write ys i (f x)
    commitBatch ys consumed produced
    writeCounter c produced
  return ys
deploy (Fold l f s00) g xs = do
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
          -- XXX: For debugging:
          delay <- randomRIO (50000, 3000000)
          threadDelay delay
          write ys i y
          return s'
        commitBatch ys consumed produced
        writeCounter c produced
        go s'
  _pid <- forkIO (go s00)
  return ys
deploy (_ :+++ _) _ _ = undefined
deploy (_ :||| _) _ _ = undefined
deploy (Fork _)   _ _ = undefined
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
        -- XXX: For debugging:
        delay <- randomRIO (50000, 3000000)
        threadDelay delay
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
        stopping <- readIORef stop
        consumed <- readCounter c
        produced <- readCursor ys
        -- NOTE: `waitFor` is inlined here, so that we can stop.
        if stopping /= -1 && consumed == stopping
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
    t <- getCurrentTime
    drawGraph g (dir </> "wc-" ++ formatTime defaultTimeLocale dateFormat t ++ ".dot")
    runDot dir
    runFeh dir
runFlow (List _xs0 _p) = undefined
  {- do
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
-}

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
