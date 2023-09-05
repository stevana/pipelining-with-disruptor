{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}

module Model where

import Data.Kind
import Control.Category
import Control.Arrow
import Data.Either

------------------------------------------------------------------------

infixr 1 :>>>
infixr 3 :&&&

data P :: Type -> Type -> Type where
  Id      :: P a a
  (:>>>)  :: P a b -> P b c -> P a c
  Map     :: (a -> b) -> P a b
  (:***)  :: P a c -> P b d -> P (a, b) (c, d)
  (:&&&)  :: P a b -> P a c -> P a (b, c)
  (:+++)  :: P a c -> P b d -> P (Either a b) (Either c d)
  (:|||)  :: P a c -> P b c -> P (Either a b) c

------------------------------------------------------------------------

examplePipeline :: P Int (Int, Bool)
examplePipeline = Id :&&& Map even

------------------------------------------------------------------------

model :: P a b -> [a] -> [b]
model Id         xs  = xs
model (f :>>> g) xs  = model g (model f xs)
model (Map f)    xs  = map f xs
model (f :*** g) xys =
  let
    (xs, ys) = unzip xys
  in
    zip (model f xs) (model g ys)
model (f :&&& g) xs = zip (model f xs) (model g xs)
model (f :+++ g) es0 =
  let
    (xs, ys) = partitionEithers es0
  in
    merge es0 (model f xs) (model g ys)
  where
    merge []             []       []       = []
    merge (Left  _ : es) (l : ls) rs       = Left  l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = Right r : merge es ls rs
    merge _ _ _ = error "impossible"
model (f :||| g) es0 =
  let
    (xs, ys) = partitionEithers es0
  in
    merge es0 (model f xs) (model g ys)
  where
    merge []             []       []       = []
    merge (Left  _ : es) (l : ls) rs       = l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = r : merge es ls rs
    merge _ _ _ = error "impossible"

example :: [Int] -> [(Int, Bool)]
example = model examplePipeline

------------------------------------------------------------------------

instance Category P where
  id    = Id
  g . f = f :>>> g

instance Arrow P where
  arr     = Map
  f *** g = f :*** g
  f &&& g = f :&&& g

instance ArrowChoice P where
  f +++ g = f :+++ g
  f ||| g = f :||| g
