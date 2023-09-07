{-# LANGUAGE ExistentialQuantification #-}

module Visualise where

import Control.Monad
import qualified Data.ByteString as BS
import Data.IORef
import Data.List (intersperse, sort)
import System.IO
import System.Process
import Data.Coerce
import System.Directory
import System.FilePath (takeExtension, (</>))

import Counter
import CRC32
import RingBufferClass

------------------------------------------------------------------------

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
