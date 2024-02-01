{-# LANGUAGE BangPatterns #-}
module Database.LSMTree.Internal.Bloomfilter where 

import Data.Word (Word32)

import qualified Data.BloomFilter as BF

data BloomFilterHeader = BloomFilterHeader
    { bfhVersion   :: !Word32
    , bfhHashCount :: !Word32
    , bfhBitSize   :: !Word32
    , bfhEndianess :: !Word32
    }

writeBloomFilter :: FilePath -> BF.Bloom a -> IO ()
writeBloomFilter fp bf = do
    return ()
  where
    !header = BloomFilterHeader
        { bfhVersion = 0
        , bfhHashCount = fromIntegral 0 -- TODO
        , bfhBitSize = fromIntegral (BF.length bf)
        , bfhEndianess = 0 -- TODO
        }

readBloomFilter :: IO (BF.Bloom a)
readBloomFilter = fail "TODO"
