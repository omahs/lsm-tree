{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.RawPage (
    -- * Main test tree
    tests,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Maybe (isJust)
import           Data.Primitive.ByteArray (ByteArray (..), byteArrayFromList)
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as P
import           Data.Word (Word16, Word32, Word64, Word8)
import           GHC.Word (byteSwap16)
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))
import           Test.Tasty.QuickCheck

import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.RawPage
import           FormatPage (BlobRef (..), Key (..), Operation (..),
                     PageLogical (..), Value (..), encodePage, serialisePage)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RawPage"
    [ testCase "empty" $ do
        -- directory:
        -- * 0 keys,
        -- * 0 blobs
        -- * key's offset offset at 8 bytes (as bitmaps are empty)
        -- * nil spare
        --
        -- and
        -- offset past last value.
        let bytes :: [Word16]
            bytes = [0, 0, 8, 0, 10]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? fst (toRawPage (PageLogical []))
        rawPageNumKeys page @=? 0
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [10]
        rawPageValueOffsets page @=? P.fromList [10]

    , testCase "single-insert" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 0, 0, 0, 0  -- ...
                , 32                      -- key offsets
                , 34, 36                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                , byteSwap16 0x8899       -- value
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? fst (toRawPage (PageLogical [(Key "\x42\x43", Insert (Value "\x88\x99"), Nothing)]))
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 36)
        rawPageHasBlobRefAt page 0 @=? 0
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (P.fromList [0x42, 0x43])

    , testCase "single-insert-blobref" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 1, 36, 0             -- directory
                , 1, 0, 0, 0              -- has blobrefs
                , 0, 0, 0, 0              -- ops
                , 0xff, 0, 0, 0
                , 0xfe, 0
                , 44                      -- key offsets
                , 46, 48                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                , byteSwap16 0x8899       -- value
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? fst (toRawPage (PageLogical [(Key "\x42\x43", Insert (Value "\x88\x99"), Just (BlobRef 0xff 0xfe))]))
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 1
        rawPageKeyOffsets page @=? P.fromList [44, 46]
        rawPageValueOffsets1 page @=? (46, 48)
        rawPageHasBlobRefAt page 0 @=? 1
        rawPageBlobRefIndex page 0 @=? (0xff,0xfe)
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (P.fromList [0x42, 0x43])

    , testCase "single-delete" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 2, 0, 0, 0  -- ...
                , 32                      -- key offsets
                , 34, 34                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? fst (toRawPage (PageLogical [(Key "\x42\x43", Delete, Nothing)]))
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 34)
        rawPageHasBlobRefAt page 0 @=? 0
        rawPageOpAt page 0 @=? 2
        rawPageKeys page @=? V.singleton (P.fromList [0x42, 0x43])

    , testCase "double-mupsert" $ do
        let bytes :: [Word16]
            bytes =
                [ 2, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 5, 0, 0, 0  -- ...
                , 34, 36                  -- key offsets
                , 38, 40, 42              -- value offsets
                , byteSwap16 0x4243       -- key 1
                , byteSwap16 0x5253       -- key 2
                , byteSwap16 0x4445       -- value 1
                , byteSwap16 0x5455       -- value 2
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? fst (toRawPage (PageLogical [(Key "\x42\x43", Mupsert (Value "\x44\x45"), Nothing), (Key "\x52\x53", Mupsert (Value "\x54\x55"), Nothing)]))
        rawPageNumKeys page @=? 2
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [34, 36, 38]
        rawPageValueOffsets page @=? P.fromList [38, 40, 42]
        rawPageHasBlobRefAt page 0 @=? 0
        rawPageHasBlobRefAt page 1 @=? 0
        rawPageOpAt page 0 @=? 1
        rawPageOpAt page 1 @=? 1
        rawPageKeys page @=? V.fromList [P.fromList [0x42, 0x43], P.fromList [0x52, 0x53]]
        rawPageValues page @=? V.fromList [P.fromList [0x44, 0x45], P.fromList [0x54, 0x55]]

        rawPageEntry page (P.fromList [0x52, 0x53]) @=? Just (Entry.Mupdate (P.fromList [0x54,0x55]))
        rawPageEntry page (P.fromList [0x99, 0x99]) @=? Nothing

    , testProperty "keys" prop_keys
    , testProperty "values" prop_values
    , testProperty "hasblobrefs" prop_hasblobrefs
    , testProperty "blobrefs" prop_blobrefs
    , testProperty "ops" prop_ops
    , testProperty "entries" prop_entries_exists
    , testProperty "missing" prop_entries_all
    , testProperty "big-insert" prop_big_insert
    , testProperty "entry" prop_single_entry
    ]

toRawPage :: PageLogical -> (RawPage, BS.ByteString)
toRawPage p = (page, sfx)
  where
    bs = serialisePage $ encodePage p
    (pfx, sfx) = BS.splitAt 4096 bs -- hardcoded page size.
    page = case SBS.toShort pfx of SBS.SBS ba -> makeRawPage (ByteArray ba) 0

prop_keys :: PageLogical -> Property
prop_keys p@(PageLogical xs) =
    rawPageKeys rawpage === V.fromList keys
  where
    rawpage = fst $ toRawPage p

    keys :: [P.Vector Word8]
    keys = [ P.fromList (BS.unpack bs) | (Key bs, _, _) <- xs ]

prop_values :: PageLogical -> Property
prop_values p@(PageLogical xs) =
    length xs /= 1 ==> rawPageValues rawpage === V.fromList values
  where
    rawpage = fst $ toRawPage p

    values :: [P.Vector Word8]
    values = [ extractValue op | (_, op, _) <- xs ]

    extractValue (Insert (Value bs))  = bsToVector bs
    extractValue (Mupsert (Value bs)) = bsToVector bs
    extractValue Delete               = P.empty

prop_blobrefs :: PageLogical -> Property
prop_blobrefs p@(PageLogical xs) =
    [rawPageBlobRefIndex rawpage i | i <- [0 .. length blobRefs - 1] ] === blobRefs
  where
    rawpage = fst $ toRawPage p

    blobRefs :: [(Word64,Word32)]
    blobRefs = [ (x, y) | (_, _, Just (BlobRef x y)) <- xs ]

prop_hasblobrefs :: PageLogical -> Property
prop_hasblobrefs p@(PageLogical xs) =
    [ rawPageHasBlobRefAt rawpage i /= 0 | i <- [0 .. length xs - 1] ] === blobRefs
  where
    rawpage = fst $ toRawPage p

    blobRefs :: [Bool]
    blobRefs = [ isJust mb | (_, _, mb) <- xs ]

prop_ops :: PageLogical -> Property
prop_ops p@(PageLogical xs) =
    [ rawPageOpAt rawpage i | i <- [0 .. length xs - 1] ] === ops
  where
    rawpage = fst $ toRawPage p

    ops :: [Word64]
    ops = [ fromOp o | (_, o, _) <- xs ]

    fromOp :: Operation -> Word64
    fromOp Insert {}  = 0
    fromOp Delete {}  = 2
    fromOp Mupsert {} = 1

prop_entries_exists :: PageLogical -> Property
prop_entries_exists (PageLogical xs) =
    length xs > 1 ==> forAll (elements xs) \(Key k, op, blobref) ->
    rawPageEntry rawpage (bsToVector k) === Just case op of
        Insert (Value v)       -> case blobref of
            Nothing            -> Entry.Insert (bsToVector v)
            Just (BlobRef x y) -> Entry.InsertWithBlob (bsToVector v) (x, y)
        Mupsert (Value v)      -> Entry.Mupdate (bsToVector v)
        Delete                 -> Entry.Delete
  where
    rawpage = fst $ toRawPage (PageLogical xs)

prop_entries_all :: PageLogical -> BS.ByteString -> Property
prop_entries_all page@(PageLogical xs) bs =
    length xs /= 1 ==> rawPageEntry rawpage k === expected
  where
    k = bsToVector bs
    rawpage = fst $ toRawPage page

    lookup3 :: Eq a => a -> [(a,b,c)] -> Maybe (b, c)
    lookup3 _ []            = Nothing
    lookup3 a ((a',b,c):zs) = if a == a' then Just (b, c) else lookup3 a zs

    expected :: Maybe (Entry.Entry (P.Vector Word8) (Word64, Word32))
    expected = case lookup3 (Key (bsFromVector k)) xs of
        Nothing                                     -> Nothing
        Just (Insert (Value v), Nothing)            -> Just (Entry.Insert (bsToVector v))
        Just (Insert (Value v), Just (BlobRef x y)) -> Just (Entry.InsertWithBlob (bsToVector v) (x, y))
        Just (Mupsert (Value v), _)                 -> Just (Entry.Mupdate (bsToVector v))
        Just (Delete, _)                            -> Just Entry.Delete

prop_big_insert :: Key -> Maybe BlobRef -> Property
prop_big_insert k blobref =
    rawPageValue1Prefix rawpage === case blobref of
        Nothing            -> Entry.Insert (bsToVector (BS.take size v), fromIntegral sfxSize)
        Just (BlobRef x y) -> Entry.InsertWithBlob (bsToVector (BS.take size v), fromIntegral sfxSize) (x, y)
  where
    page = PageLogical [(k, Insert (Value v), blobref)]
    (rawpage, sfx) = toRawPage page

    -- original value
    v = BS.replicate 5000 42

    size :: Int
    size = 5000 - sfxSize

    sfxSize :: Int
    sfxSize = BS.length sfx

prop_single_entry :: Key -> Operation -> Maybe BlobRef -> Property
prop_single_entry k op blobref = label (show $ BS.null sfx) $
    rawPageValue1Prefix rawpage === case op of
        Insert (Value v)       -> case blobref of
            Nothing            -> Entry.Insert (bsToVector (trim v), fromIntegral sfxSize)
            Just (BlobRef x y) -> Entry.InsertWithBlob (bsToVector (trim v), fromIntegral sfxSize) (x, y)
        Mupsert (Value v)      -> Entry.Mupdate (bsToVector (trim v), fromIntegral sfxSize)
        Delete                 -> Entry.Delete
  where
    page = PageLogical [(k, op, blobref)]
    (rawpage, sfx) = toRawPage page

    trim :: BS.ByteString -> BS.ByteString
    trim = BS.dropEnd sfxSize

    sfxSize :: Int
    sfxSize = BS.length sfx

bsToVector :: BS.ByteString -> P.Vector Word8
bsToVector = P.fromList . BS.unpack

bsFromVector :: P.Vector Word8 -> BS.ByteString
bsFromVector = BS.pack . P.toList
