{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use camelCase" -}

module Database.LSMTree.Generators (
    -- * WithSerialised
    WithSerialised (..)
    -- * UTxO keys
  , UTxOKey (..)
    -- * Range-finder precision
  , RFPrecision (..)
  , rfprecInvariant
    -- * A (logical\/true) page
    -- ** A true page
  , TruePageSummary (..)
  , flattenLogicalPageSummary
    -- ** A logical page
  , LogicalPageSummary (..)
  , shrinkLogicalPageSummary
  , toAppend
    -- * Sequences of (logical\/true) pages
  , Pages (..)
  , optimiseRFPrecision
    -- ** Sequences of true pages
  , TruePageSummaries
  , flattenLogicalPageSummaries
    -- ** Sequences of logical pages
  , LogicalPageSummaries
  , toAppends
  , labelPages
  , shrinkPages
  , genPages
  , mkPages
  , pagesInvariant
    -- * Chunking size
  , ChunkSize (..)
  , chunkSizeInvariant
  ) where

import           Control.DeepSeq (NFData)
import           Data.Coerce (coerce)
import           Data.Containers.ListUtils (nubOrd)
import           Data.List (sort)
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           Database.LSMTree.Internal.Run.Index.Compact (Append (..),
                     rangeFinderPrecisionBounds, suggestRangeFinderPrecision)
import           Database.LSMTree.Internal.Serialise (Serialise (..),
                     SerialisedKey, topBits16)
import           Database.LSMTree.Util
import           Database.LSMTree.Util.Orphans ()
import           GHC.Generics (Generic)
import           System.Random (Uniform)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (..), Gen, Property)
import           Test.QuickCheck.Gen (genDouble)

{-------------------------------------------------------------------------------
  WithSerialised
-------------------------------------------------------------------------------}

-- | Cach serialised keys
--
-- Also useful for failing tests that have keys as inputs, because the printed
-- 'WithSerialised' values will show both keys and their serialised form.
data WithSerialised k = TestKey k SerialisedKey
  deriving Show

instance Eq k => Eq (WithSerialised k) where
  TestKey k1 _ == TestKey k2 _ = k1 == k2

instance Ord k => Ord (WithSerialised k) where
  TestKey k1 _ `compare` TestKey k2 _ = k1 `compare` k2

instance (Arbitrary k, Serialise k) => Arbitrary (WithSerialised k) where
  arbitrary = do
    x <- arbitrary
    pure $ TestKey x (serialise x)
  shrink (TestKey k _) = [TestKey k' (serialise k') | k' <- shrink k]

instance Serialise (WithSerialised k) where
  serialise (TestKey _ skey) = skey

{-------------------------------------------------------------------------------
  UTxO keys
-------------------------------------------------------------------------------}

-- | A model of a UTxO key (256-bit hash)
newtype UTxOKey = UTxOKey Word256
  deriving stock (Show, Generic)
  deriving newtype ( Eq, Ord, NFData
                   , Hashable, Serialise
                   , Num, Enum, Real, Integral
                   )
  deriving anyclass Uniform

instance Arbitrary UTxOKey where
  arbitrary = UTxOKey <$>
      (Word256 <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary)
  shrink (UTxOKey w256) = [
        UTxOKey w256'
      | let i256 = toInteger w256
      , i256' <- shrink i256
      , toInteger (minBound :: Word256) <= i256'
      , toInteger (maxBound :: Word256) >= i256'
      , let w256' = fromIntegral i256'
      ]

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving stock (Show, Generic)
  deriving newtype Num
  deriving anyclass NFData

instance Arbitrary RFPrecision where
  arbitrary = RFPrecision <$> QC.chooseInt (rfprecLB, rfprecUB)
    where (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds
  shrink (RFPrecision x) =
      [RFPrecision x' | x' <- shrink x , rfprecInvariant (RFPrecision x')]

rfprecInvariant :: RFPrecision -> Bool
rfprecInvariant (RFPrecision x) = x >= rfprecLB && x <= rfprecUB
  where (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds

{-------------------------------------------------------------------------------
  True page
-------------------------------------------------------------------------------}

-- | A summary of min/max information for keys on a /true/ page.
--
-- A ture page corresponds directly to a disk page. See 'LogicalPageSummary' for
-- contrast.
data TruePageSummary k = TruePageSummary { tpsMinKey :: k, tpsMaxKey :: k }

flattenLogicalPageSummary :: LogicalPageSummary k -> [TruePageSummary k]
flattenLogicalPageSummary = \case
    OnePageOneKey k       -> [TruePageSummary k k]
    OnePageManyKeys k1 k2 -> [TruePageSummary k1 k2]
    MultiPageOneKey k n   -> replicate (fromIntegral n+1) (TruePageSummary k k)

{-------------------------------------------------------------------------------
  Logical page
-------------------------------------------------------------------------------}

-- | A summary of min/max information for keys on a /logical/ page.
--
-- A key\/operation pair can fit onto a single page, or the operation is so
-- large that its bytes flow over into subsequent pages. A logical page makes
-- this overflow explicit. Making these cases explicit in the representation
-- makes generating and shrinking test cases easier.
data LogicalPageSummary k =
    OnePageOneKey   k
  | OnePageManyKeys k k
  | MultiPageOneKey k Word32 -- ^ number of overflow pages
  deriving stock (Show, Generic, Functor)
  deriving anyclass NFData

toAppend :: LogicalPageSummary SerialisedKey -> Append
toAppend (OnePageOneKey k)       = AppendSinglePage k k
toAppend (OnePageManyKeys k1 k2) = AppendSinglePage k1 k2
toAppend (MultiPageOneKey k n)   = AppendMultiPage k n

shrinkLogicalPageSummary :: Arbitrary k => LogicalPageSummary k -> [LogicalPageSummary k]
shrinkLogicalPageSummary = \case
    OnePageOneKey k       -> OnePageOneKey <$> shrink k
    OnePageManyKeys k1 k2 -> OnePageManyKeys <$> shrink k1 <*> shrink k2
    MultiPageOneKey k n   -> [MultiPageOneKey k' n | k' <- shrink k]
                          <> [MultiPageOneKey k n' | n' <- shrink n]

{-------------------------------------------------------------------------------
  Sequences of (logical\/true) pages
-------------------------------------------------------------------------------}

-- | Sequences of (logical\/true) pages
--
-- INVARIANT: The sequence consists of multiple pages in sorted order (keys are
-- sorted within a page and across pages). Pages are partitioned, meaning all
-- keys inside a page have the same range-finder bits.
data Pages fp k = Pages {
    getRangeFinderPrecision :: RFPrecision
  , getPages                :: [fp k]
  }
  deriving stock (Show, Generic, Functor)
  deriving anyclass NFData

class TrueNumberOfPages fp where
  trueNumberOfPages :: Pages fp k -> Int

instance TrueNumberOfPages LogicalPageSummary where
  trueNumberOfPages :: LogicalPageSummaries k -> Int
  trueNumberOfPages = length . getPages . flattenLogicalPageSummaries

instance TrueNumberOfPages TruePageSummary where
  trueNumberOfPages :: TruePageSummaries k -> Int
  trueNumberOfPages = length . getPages

optimiseRFPrecision :: TrueNumberOfPages fp => Pages fp k -> Pages fp k
optimiseRFPrecision ps = ps {
      getRangeFinderPrecision = coerce $ suggestRangeFinderPrecision (trueNumberOfPages ps)
    }

{-------------------------------------------------------------------------------
  Sequences of true pages
-------------------------------------------------------------------------------}

type TruePageSummaries    k = Pages TruePageSummary k

flattenLogicalPageSummaries :: LogicalPageSummaries k -> TruePageSummaries k
flattenLogicalPageSummaries (Pages f ps) = Pages f (concatMap flattenLogicalPageSummary ps)

{-------------------------------------------------------------------------------
  Sequences of logical pages
-------------------------------------------------------------------------------}

type LogicalPageSummaries k = Pages LogicalPageSummary k

toAppends :: Serialise k => LogicalPageSummaries k -> [Append]
toAppends (Pages _ ps) = fmap (toAppend . fmap serialise) ps

--
-- Labelling
--

labelPages :: LogicalPageSummaries k -> (Property -> Property)
labelPages ps =
      QC.tabulate "RFPrecision: optimal" [show suggestedRfprec]
    . QC.tabulate "RFPrecision: actual" [show actualRfprec]
    . QC.tabulate "RFPrecision: |optimal-actual|" [show dist]
    . QC.tabulate "# True pages" [showPowersOf10 nTruePages]
    . QC.tabulate "# Logical pages" [showPowersOf10 nLogicalPages]
    . QC.tabulate "# OnePageOneKey logical pages" [showPowersOf10 n1]
    . QC.tabulate "# OnePageManyKeys logical pages" [showPowersOf10 n2]
    . QC.tabulate "# MultiPageOneKey logical pages" [showPowersOf10 n3]
  where
    nLogicalPages = length $ getPages ps
    nTruePages = trueNumberOfPages ps
    actualRfprec = getRangeFinderPrecision ps
    suggestedRfprec = getRangeFinderPrecision $ optimiseRFPrecision ps
    dist = abs (suggestedRfprec - actualRfprec)

    (n1,n2,n3) = counts (getPages ps)

    counts :: [LogicalPageSummary k] -> (Int, Int, Int)
    counts []       = (0, 0, 0)
    counts (lp:lps) = let (x, y, z) = counts lps
                      in case lp of
                        OnePageOneKey{}   -> (x+1, y, z)
                        OnePageManyKeys{} -> (x, y+1, z)
                        MultiPageOneKey{} -> (x, y, z+1)

--
-- Generation and shrinking
--

instance (Arbitrary k, Ord k, Serialise k) => Arbitrary (LogicalPageSummaries k) where
  arbitrary = genPages 0.03 (QC.choose (0, 16))
  shrink = shrinkPages

shrinkPages :: (Arbitrary k, Ord k, Serialise k) => LogicalPageSummaries k -> [LogicalPageSummaries k]
shrinkPages (Pages rfprec ps) = [
      Pages rfprec ps'
    | ps' <- QC.shrinkList shrinkLogicalPageSummary ps, pagesInvariant (Pages rfprec ps')
    ] <> [
      Pages rfprec' ps
    | rfprec' <- shrink rfprec, pagesInvariant (Pages rfprec' ps)
    ]

genPages ::
     (Arbitrary k, Ord k, Serialise k)
  => Double -- ^ Probability of a value being larger-than-page
  -> Gen Word32 -- ^ Number of overflow pages for a larger-than-page value
  -> Gen (LogicalPageSummaries k)
genPages p genN = do
    rfprec <- arbitrary
    ks <- arbitrary
    mkPages p genN rfprec ks

mkPages ::
     forall k. (Ord k, Serialise k)
  => Double -- ^ Probability of a value being larger-than-page
  -> Gen Word32 -- ^ Number of overflow pages for a larger-than-page value
  -> RFPrecision
  -> [k]
  -> Gen (LogicalPageSummaries k)
mkPages p genN rfprec@(RFPrecision n) =
    fmap (Pages rfprec) . go . nubOrd . sort
  where
    go :: [k] -> Gen [LogicalPageSummary k]
    go []          = pure []
    go [k]         = do b <- largerThanPage
                        if b then pure . MultiPageOneKey k <$> genN else pure [OnePageOneKey k]
                   -- the min and max key are allowed to be the same
    go  (k1:k2:ks) = do b <- largerThanPage
                        if | b
                           -> (:) <$> (MultiPageOneKey k1 <$> genN) <*> go (k2 : ks)
                           | topBits16 n (serialise k1) == topBits16 n (serialise k2)
                           -> (OnePageManyKeys k1 k2 :) <$> go ks
                           | otherwise
                           -> (OnePageOneKey k1 :) <$>  go (k2 : ks)

    largerThanPage :: Gen Bool
    largerThanPage = genDouble >>= \x -> pure (x < p)

pagesInvariant :: (Ord k, Serialise k) => LogicalPageSummaries k -> Bool
pagesInvariant (Pages (RFPrecision rfprec) ps0) =
       sort ks   == ks
    && nubOrd ks == ks
    && all partitioned ps0
  where
    ks = flatten ps0
    partitioned = \case
      OnePageOneKey _       -> True
      OnePageManyKeys k1 k2 -> topBits16 rfprec (serialise k1) == topBits16 rfprec (serialise k2)
      MultiPageOneKey _ _   -> True

    flatten :: Eq k => [LogicalPageSummary k] -> [k]
    flatten []            = []
                          -- the min and max key are allowed to be the same
    flatten (p:ps) = case p of
      OnePageOneKey k       -> k : flatten ps
      OnePageManyKeys k1 k2 -> k1 : k2 : flatten ps
      MultiPageOneKey k _   -> k : flatten ps

{-------------------------------------------------------------------------------
  Chunking size
-------------------------------------------------------------------------------}

newtype ChunkSize = ChunkSize Int
  deriving stock Show
  deriving newtype Num

instance Arbitrary ChunkSize where
  arbitrary = ChunkSize <$> QC.chooseInt (chunkSizeLB, chunkSizeUB)
  shrink (ChunkSize csize) = [
        ChunkSize csize'
      | csize' <- shrink csize
      , chunkSizeInvariant (ChunkSize csize')
      ]

chunkSizeLB, chunkSizeUB :: Int
chunkSizeLB = 1
chunkSizeUB = 20

chunkSizeInvariant :: ChunkSize -> Bool
chunkSizeInvariant (ChunkSize csize) = chunkSizeLB <= csize && csize <= chunkSizeUB
