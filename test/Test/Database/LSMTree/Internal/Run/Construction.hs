{-# LANGUAGE BangPatterns #-}

module Test.Database.LSMTree.Internal.Run.Construction (tests) where

import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short as SBS
import           Data.ByteString.Short.Internal (ShortByteString (SBS))
import           Data.Foldable (Foldable (..))
import           Data.Maybe (fromJust)
import           Data.Primitive.ByteArray (ByteArray (ByteArray),
                     sizeofByteArray)
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.Run.Construction as Real
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     fromShortByteString)
import qualified FormatPage as Proto
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Run.Construction" [
      testProperty "prop_BitMapMatchesPrototype"   prop_BitMapMatchesPrototype
    , testProperty "prop_CrumbMapMatchesPrototype" prop_CrumbMapMatchesPrototype
    , largerTestCases $
      testProperty "prop_paddedToDiskPageSize with trivially partitioned pages" $
        prop_paddedToDiskPageSize 0
    , largerTestCases $
      testProperty "prop_paddedToDiskPageSize with partitioned pages" $
        prop_paddedToDiskPageSize 8
    , largerTestCases $
      testProperty "prop_pageBuilderMatchesPrototype" prop_pageBuilderMatchesPrototype
    ]
  where largerTestCases = localOption (QuickCheckMaxSize 500) . localOption (QuickCheckTests 10000)

-- Constructing a bitmap incrementally yields the same result as constructing it
-- in one go
prop_BitMapMatchesPrototype :: [Bool] -> Property
prop_BitMapMatchesPrototype bs = counterexample "reverse real /= model" $ reverse real === model
  where
    real  = unStricterList $ bmbits $ foldl' (\acc b -> appendBit (boolToBit b) acc) emptyBitMap bs
    model = Proto.toBitmap bs

    boolToBit False = 0
    boolToBit True  = 1

-- Constructing a crumbmap incrementally yields the same result as constructing
-- it in one go
prop_CrumbMapMatchesPrototype :: [(Bool, Bool)] -> Property
prop_CrumbMapMatchesPrototype bs = counterexample "reverse real /= model" $ reverse real === model
  where
    real  = unStricterList $ cmbits $
            foldl' (\acc b -> appendCrumb (boolsToCrumb b) acc) emptyCrumbMap bs
    model = Proto.toBitmap (concatMap (\(b1, b2) -> [b1, b2]) bs)

    -- assumes the booleans in the tuple are in little-endian order
    boolsToCrumb (False, False) = 0
    boolsToCrumb (True , False) = 1
    boolsToCrumb (False, True ) = 2
    boolsToCrumb (True , True ) = 3

prop_paddedToDiskPageSize :: Int -> PageLogical' -> Property
prop_paddedToDiskPageSize rfp page =
    counterexample "expected number of output bytes to be of disk page size" $
    tabulate "page size in bytes" [show $ LBS.length bytes] $
    LBS.length bytes `rem` 4096 === 0
  where
    bytes = BB.toLazyByteString . pageBuilder $ fromListPageAcc' rfp (getRealKOps page)

prop_pageBuilderMatchesPrototype :: PageLogical' -> Property
prop_pageBuilderMatchesPrototype page = counterexample "real /= model" $ real === model
  where
    model = Proto.serialisePage (Proto.encodePage $ getPageLogical' page)
    real  = trunc $ BS.toStrict . BB.toLazyByteString . pageBuilder $ fromListPageAcc (getRealKOps page)

    -- truncate padding on the real page
    trunc = BS.take (BS.length model)

{-------------------------------------------------------------------------------
  Util
-------------------------------------------------------------------------------}

fromListPageAcc :: [(SerialisedKey, Entry RawValue BlobRef)] -> PageAcc
fromListPageAcc = fromListPageAcc' 0

fromListPageAcc' :: Int -> [(SerialisedKey, Entry RawValue BlobRef)] -> PageAcc
fromListPageAcc' rfp kops = fromJust $ go (paEmpty rfp) kops
  where
    -- Add keys until full
    go !pacc [] = Just pacc
    go !pacc ((k, e):kops') =
      case paAddElem k e pacc of
        Nothing    -> Just pacc
        Just pacc' -> go pacc' kops'


fromProtoKOp ::
     (Proto.Key, Proto.Operation, Maybe Proto.BlobRef)
  -> (SerialisedKey, Entry RawValue BlobRef)
fromProtoKOp (k, op, mblobref) = (fromProtoKey k, bimap fromProtoValue fromProtoBlobRef e)
  where e = case op of
              Proto.Insert v  -> case mblobref of
                  Nothing -> Insert v
                  Just br -> InsertWithBlob v br
              Proto.Mupsert v -> Mupdate v
              Proto.Delete -> Delete

fromProtoKey :: Proto.Key -> SerialisedKey
fromProtoKey (Proto.Key bs) = fromShortByteString $ SBS.toShort bs

fromProtoValue :: Proto.Value -> RawValue
fromProtoValue (Proto.Value bs) = RawValue 0 (sizeofByteArray ba) ba
  where ba = case SBS.toShort bs of SBS ba' -> ByteArray ba'

fromProtoBlobRef :: Proto.BlobRef -> BlobRef
fromProtoBlobRef (Proto.BlobRef x y) = BlobRef x y

-- | Wrapper around 'PageLogical' that only generates a k\/op pair with a blob
-- reference if the op is an insert.
newtype PageLogical' = PageLogical' {getPageLogical' :: Proto.PageLogical}
  deriving Show

getRealKOps :: PageLogical' -> [(SerialisedKey, Entry RawValue BlobRef)]
getRealKOps = fmap fromProtoKOp . getPrototypeKOps

getPrototypeKOps :: PageLogical' -> [(Proto.Key, Proto.Operation, Maybe Proto.BlobRef)]
getPrototypeKOps (PageLogical' (Proto.PageLogical kops)) = kops

instance Arbitrary PageLogical' where
  arbitrary = PageLogical' . demoteBlobRefs <$> arbitrary
  shrink (PageLogical' page) = [ PageLogical' (demoteBlobRefs page')
                               | page' <- shrink page ]

demoteBlobRefs :: Proto.PageLogical -> Proto.PageLogical
demoteBlobRefs (Proto.PageLogical kops) = Proto.PageLogical (fmap demoteBlobRef kops)

demoteBlobRef ::
     (Proto.Key, Proto.Operation, Maybe Proto.BlobRef)
  -> (Proto.Key, Proto.Operation, Maybe Proto.BlobRef)
demoteBlobRef (k, op, mblobref) = case op of
    Proto.Insert{} -> (k, op, mblobref)
    _              -> (k, op, Nothing)
