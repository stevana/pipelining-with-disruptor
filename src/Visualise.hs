{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}

module Visualise where

import Control.Monad
import qualified Data.ByteString as BS
import Data.Coerce
import qualified Data.Map as Map
import Data.IORef
import Data.Function (on)
import Data.List (sort, groupBy, sortBy)
import qualified Data.Text.Lazy.IO as T
import System.Directory
import System.FilePath (takeExtension, (</>))
import System.IO
import System.Process
import Data.Text.Lazy.Builder

import Counter
import CRC32
import RingBufferClass
import Sharding

------------------------------------------------------------------------

data Node
  = forall a. (HasRB a, Show a) => RingBufferNode Label Sharding (RB a)
  | WorkerNode Label Counter
  | SourceOrSinkNode Label
  | ProducerNode Label

instance Show Node where
  show = show . nodeLabel

nodeLabel :: Node -> Label
nodeLabel (RingBufferNode l _ _) = l
nodeLabel (WorkerNode l _)       = l
nodeLabel (SourceOrSinkNode l)   = l
nodeLabel (ProducerNode l)       = l

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

addRingBufferNode :: (HasRB a, Show a) => Graph -> Label -> Sharding -> RB a -> IO ()
addRingBufferNode g l s rb = modifyIORef' (gNodes g) (RingBufferNode l s rb :)

addEdge :: Graph -> Edge -> IO ()
addEdge g e = modifyIORef' (gEdges g) (e :)

addProducers :: Graph -> Label -> [Label] -> IO ()
addProducers g src dsts = mapM_ (\dst -> modifyIORef' (gEdges g) (Produces src dst :)) dsts

addConsumers :: Graph -> [Label] -> Label -> IO ()
addConsumers g dsts src = mapM_ (\dst -> modifyIORef' (gEdges g) (Consumes src dst :)) dsts

combineSharded :: [Node] -> [[Node]]
combineSharded
  = groupBy ((==) `on` nodeLabel)
  . sortBy (compare `on` nodeLabel)

drawList :: [String] -> Builder
drawList xs0 = fromText "{ " <> go xs0 <> fromText " }"
  where
    go []       = mempty
    go [x]      = fromString x
    go (x : xs) = fromString x <> fromText " | " <> go xs

drawLists :: [[String]] -> Builder
drawLists []         = mempty
drawLists [xs]       = drawList xs
drawLists (xs : xss) = drawList xs <> " | " <> drawLists xss

drawGraph :: Graph -> FilePath -> IO ()
drawGraph g fp = do
  nodes <- reverse <$> readIORef (gNodes g)
  edges <- reverse <$> readIORef (gEdges g)

  nodes' <- forM (combineSharded nodes) $ \node ->
    case node of
      rbns@(RingBufferNode l _ _ : _) -> do
        ixss <- forM rbns $ \(RingBufferNode _l s rb) -> do
          i  <- readCounter (cursor rb)
          xs <- map show <$> toListSharded rb s
          return (i, xs)
        let (is, xss) = unzip ixss
            k         = maximum is
            elements  = drawLists xss
        return (l, fromText "  " <> fromString (coerce l) <> fromText " [shape=Mrecord label=\"<lbl> " <>
                fromString (coerce l) <> fromText " | " <> elements <> " | <seq> " <>
                fromString (show k) <> fromText "\"]\n")
      wns@(WorkerNode l _ : _) -> do
        is <- forM wns $ \(WorkerNode _l c) ->
          readCounter c
        let i = minimum is
        return (l, fromText "  " <> fromString (coerce l) <> fromText " [shape=record label=\"<lbl> " <>
                fromString (coerce l) <> fromText " | <seq> " <> fromString (show i) <> fromText "\"]\n")
      [ProducerNode l] ->
        return (l ,fromText "  " <> fromString (coerce l) <> fromText " [shape=record label=\"<lbl> " <>
                fromString (coerce l) <> fromText "\"]\n")
      [SourceOrSinkNode l] ->
        return (l, fromText "  " <> fromString (coerce l) <> fromText " [shape=none]\n")
      _otherwise -> error (show _otherwise)

  let sourceOrSink = foldMap (\node -> case node of
                                         SourceOrSinkNode l -> [l]
                                         _otherwise -> []) nodes
  edges' <- forM edges $ \edge ->
    case edge of
      Produces from to ->
        return (fromText "  " <> fromString (coerce from) <> fromText " -> " <>
                fromString (coerce to) <> if to `elem` sourceOrSink then fromText "\n" else fromText ":lbl\n")
      Consumes from to -> return (fromText "  " <> fromString (coerce from) <> " -> " <>
                                  fromString (coerce to) <> fromText ":seq [style=dashed]\n")

  -- Preserve the original order in which nodes appear.
  let texts   = Map.fromList nodes'
      nodes'' = map (\n -> texts Map.! nodeLabel n) nodes

  let builder = mconcat
        [ fromText "digraph g {\n"
        , mconcat nodes''
        , singleton '\n'
        , mconcat edges'
        , singleton '}'
        ]

  T.writeFile fp (toLazyText (builder))

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
