{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Data.List (nub, sort)
import           Data.Word (Word64)
import qualified Database.LSMTree.Internal.Run.Index.Basic as Basic
import           Database.LSMTree.Internal.Run.Index.Compact
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Run.Index.Compact" [
      testGroup "Pages (not partitioned)" [
          testProperty "Arbitrary satisfies invariant" $
            property . pagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all pagesInvariant . shrink @(Pages Word64)
        ]
    , testGroup "Pages (partitioned)" [
          testProperty "Arbitrary satisfies invariant" $
            property . partitionedPagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all partitionedPagesInvariant . shrink @(PartitionedPages Word64)
        ]
    , testProperty "prop_searchMinMaxKeysAfterConstruction" $
        prop_searchMinMaxKeysAfterConstruction @Word64
    , testProperty "prop_searchMatchesModel" $
        prop_searchMatchesModel @Word64
      -- cabal run lsm-tree-test -- -p prop_searchMatchesModel --quickcheck-tests=100000 --quickcheck-max-size=1000 --quickcheck-replay=430453
    , testProperty "regress" $
        prop_searchMatchesModel @Word64
          (PartitionedPages {getRangeFinderPrecision = RFPrecision 0, getPartitionedPages = [(0,1),(9250810581454159873,9250810581454159874)]})
          [9250810581454159872]
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | After construction, searching for the minimum/maximum key of every page
-- @pageNr@ returns the @pageNr@.
--
-- Example: @search minKey (fromList rfprec [(minKey, maxKey)]) == 0@.
prop_searchMinMaxKeysAfterConstruction ::
     (SliceBits k, Integral k, Show k)
  => PartitionedPages k
  -> Property
prop_searchMinMaxKeysAfterConstruction (PartitionedPages (RFPrecision rfprec) ks) =
      classify (hasClashes ci) "Compact index contains clashes"
    $ tabulate "Range-finder bit-precision" [show rfprec]
    $ counterexample (show idxs)
    $ counterexample (dumpInternals ci)
    $ property $ all p idxs
  where
    ci = fromList rfprec ks

    f idx (minKey, maxKey) =
        ( idx
        , search minKey ci
        , search maxKey ci
        , search (minKey + (maxKey - minKey) `div` 2) ci
        )

    idxs = zipWith f [0..] ks

    p (idx, x, y, z) =
         Just idx == x && Just idx == y && Just idx == z

-- | The result of searching for a key in a compact index matches the result of
-- searching for a key in a modelled index.
prop_searchMatchesModel ::
     (SliceBits k, Integral k, Show k)
  => PartitionedPages k
  -> [k]
  -> Property
prop_searchMatchesModel (PartitionedPages (RFPrecision rfprec) ks) searchKs =
      classify (hasClashes ci) "Compact index contains clashes"
    $ tabulate "Range-finder bit-precision" [show rfprec]
    $ counterexample (dumpInternals ci)
    $ counterexample (show searchResults)
    $ property $ all p searchResults
  where
    bi = Basic.fromList ks
    ci = fromList rfprec ks

    searchResults = fmap (\k -> ( k
                                , printWord16 $ topBits16 rfprec k
                                , printWord32 $ sliceBits32 rfprec 32 k
                                , Basic.search k bi
                                , search k ci
                                )) searchKs

    -- Approximation: the model may provide more precise results than the
    -- compact index.
    p (_k, _, _, biResult, ciResult) = case biResult of
      -- If the model provides a more precise results, the property is trivially
      -- true.
      Nothing -> True
      _       -> biResult == ciResult

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving Show

instance Arbitrary RFPrecision where
  arbitrary = RFPrecision <$>
      (arbitrary `suchThat` (\x -> x >= rfprecLB && x <= rfprecUB))
    where
      (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds
  shrink (RFPrecision x) = [RFPrecision x' | x' <- shrink x
                                           , x' >= rfprecLB && x' <= rfprecUB]
    where
      (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds

{-------------------------------------------------------------------------------
  Pages (non-partitioned)
-------------------------------------------------------------------------------}

-- | We model a disk page in a run as a pair of its minimum and maximum key. A
-- run consists of multiple pages in sorted order, and keys are unique.
newtype Pages k = Pages { getPages :: [(k, k)]}
  deriving Show

instance (Arbitrary k, Ord k) => Arbitrary (Pages k) where
  arbitrary = mkPages <$> scale (2*) (arbitrary @[k])
  shrink (Pages ks) = [Pages ks' | ks' <- shrink ks, pagesInvariant (Pages ks')]

mkPages :: Ord k => [k] -> Pages k
mkPages ks0 = Pages $ go $ nub $ sort ks0
  where
    go :: [k] -> [(k, k)]
    go []             = []
    go [_]            = []
    go (k1 : k2 : ks) = (k1, k2) : go ks

pagesInvariant ::  Ord k => Pages k -> Bool
pagesInvariant (Pages ks) = nub ks' == ks' && sort ks' == ks'
  where ks' = flatten ks

flatten :: [(k, k)] -> [k]
flatten []              = []
flatten ((k1, k2) : ks) = k1 : k2 : flatten ks

{-------------------------------------------------------------------------------
  Pages (partitioned)
-------------------------------------------------------------------------------}

-- | In partitioned pages, all keys inside a page have the same range-finder
-- bits.
data PartitionedPages k = PartitionedPages {
    getRangeFinderPrecision :: RFPrecision
  , getPartitionedPages     :: [(k, k)]
  }
  deriving Show

instance (Arbitrary k, SliceBits k, Integral k) => Arbitrary (PartitionedPages k) where
  arbitrary = mkPartitionedPages <$> arbitrary <*> arbitrary
  shrink (PartitionedPages rfprec ks) = [
        PartitionedPages rfprec ks'
      | ks' <- shrink ks
      , partitionedPagesInvariant (PartitionedPages rfprec ks')
      ] <> [
        PartitionedPages rfprec' ks
      | rfprec' <- shrink rfprec
      , partitionedPagesInvariant (PartitionedPages rfprec' ks)
      ] <> [
        PartitionedPages rfprec' ks'
      | ks' <- shrink ks
      , rfprec' <- shrink rfprec
      , partitionedPagesInvariant (PartitionedPages rfprec' ks')
      ]

mkPartitionedPages ::
     (SliceBits k, Integral k)
  => RFPrecision
  -> Pages k
  -> PartitionedPages k
mkPartitionedPages rfprec0@(RFPrecision rfprec) (Pages ks) =
    PartitionedPages rfprec0 $ foldr f [] ks
  where f (kmin, kmax)   []                          = [(kmin, kmax)]
        f ks1@(kmin1, kmax1) ps@((kmin2, _kmax2) : _)
          | topBits16 rfprec kmin1 <  topBits16 rfprec kmax1
          , topBits16 rfprec kmax1 == topBits16 rfprec kmin2 = ps
          | otherwise                                        = ks1 : ps

partitionedPagesInvariant :: (SliceBits k, Integral k) => PartitionedPages k -> Bool
partitionedPagesInvariant (PartitionedPages (RFPrecision rfprec) ks) =
    pagesInvariant (Pages ks) && properPartitioning
  where
    properPartitioning = and (zipWith p ks $ drop 1 ks)
    p (kmin1, kmax1) (kmin2, _kmax2) = not $
         topBits16 rfprec kmin1 <  topBits16 rfprec kmax1
      && topBits16 rfprec kmax1 == topBits16 rfprec kmin2
