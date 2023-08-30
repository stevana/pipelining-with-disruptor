module Template (Template, template, instantiate) where

import Control.Exception (assert)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB

------------------------------------------------------------------------

newtype Template = Template [TextOrBlank]

data TextOrBlank = Blank | Text !Text
  deriving Eq

------------------------------------------------------------------------

-- | >>> template "_" "hello _"
--   [Text "hello ", Blank]@
template :: Text -> Text -> Template
template blank = go 0 [] . T.breakOnAll blank
  where
    go _n acc []              = Template (reverse acc)
    go  n acc ((x, _y) : xys) =
      go (n + (T.length x - n) + T.length blank) (Blank : Text (T.drop n x) : acc) xys

-- | >>> instantiate (template "_" "hello _") ["world!"]
--   "hello world!"
instantiate :: Template -> [Text] -> Text
instantiate (Template xs) ys =
  assert (length (filter (== Blank) xs) == length ys) $
    go xs ys mempty
  where
    go []           []       acc = TL.toStrict (TB.toLazyText acc)
    go (Blank : xs) (y : ys) acc = go xs ys (acc <> TB.fromText y)
    go (Text x : xs) ys      acc = go xs ys (acc <> TB.fromText x)
