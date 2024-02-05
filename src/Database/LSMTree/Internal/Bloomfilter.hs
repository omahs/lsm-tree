{-# LANGUAGE BangPatterns #-}
module Database.LSMTree.Internal.Bloomfilter where 

import Data.Word (Word32)
import System.IO (withFile, IOMode (..))
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS

import qualified Data.BloomFilter as BF

data BloomFilterHeader = BloomFilterHeader
    { bfhVersion   :: !Word32
    , bfhHashCount :: !Word32
    , bfhBitSize   :: !Word32
    , bfhEndianess :: !Word32
    }

bloomFilterVersion :: Word32
bloomFilterVersion = 0

toBuilder :: BF.Bloom a -> B.Builder
toBuilder bf =
    BF.word32BE (bfhVersion header) <>
    BF.word32BE (bfhHashCount header) <>
    BF.word32BE (bfhBitSize header) <>
    BF.word32BE (bfhEndianess)
  where
    !header = BloomFilterHeader
        { bfhVersion   = bloomFilterVersion
        , bfhHashCount = fromIntegral (BF.hashesN bf)
        , bfhBitSize   = fromIntegral (BF.length bf)
        , bfhEndianess = 0 -- TODO
        }

writeBloomFilter :: FilePath -> BF.Bloom a -> IO ()

readBloomFilter :: IO (BF.Bloom a)
readBloomFilter = fail "TODO"
