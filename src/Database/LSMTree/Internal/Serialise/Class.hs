-- | Public API for serialisation of keys, blobs and values
module Database.LSMTree.Internal.Serialise.Class (
    SerialiseKey (..)
  , SerialiseValue (..)
  , RawBytes (..)
  ) where

import           Database.LSMTree.Internal.Serialise.RawBytes

-- | Ordering-preserving serialisation for keys.
--
-- INVARIANT: Serialisation should preserve ordering, @x `compare` y ==
-- serialise x `compare` serialise y@. Raw bytes are lexicographically
-- ordered, in particular this means that values should be serialised into
-- big-endian formats.
class SerialiseKey k where
  {-# MINIMAL serialiseKey, deserialiseKeyOrFail #-}
  serialiseKey :: k -> RawBytes
  deserialiseKey :: RawBytes -> k
  deserialiseKeyOrFail :: RawBytes -> Maybe k

  deserialiseKey rb = case deserialiseKeyOrFail rb of
    Nothing -> error "deserialiseKey: deserialisation failure"
    Just x  -> x

-- | Serialisation of values and blobs.
class SerialiseValue v where
  {-# MINIMAL serialiseValue, deserialiseValueOrFail, deserialiseValueNOrFail #-}
  serialiseValue :: v -> RawBytes
  deserialiseValue :: RawBytes -> v
  deserialiseValueOrFail :: RawBytes -> Maybe v

  deserialiseValue rb = case deserialiseValueOrFail rb of
    Nothing -> error "deserialiseValue: deserialisation failure"
    Just x  -> x

  deserialiseValueN :: [RawBytes] -> v
  deserialiseValueNOrFail :: [RawBytes] -> Maybe v

  deserialiseValueN rbs = case deserialiseValueNOrFail rbs of
    Nothing -> error "deserialiseN: deserialisation failure"
    Just x  -> x

