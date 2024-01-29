{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
) where

import           Data.Bifoldable (bifoldMap)
import           Data.Bifunctor (bimap)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as V
import           Data.Word (Word32, Word64)
import           System.Directory (createDirectoryIfMissing)
import           System.FilePath
import           System.IO
import           System.IO.Temp
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))
import           Test.Tasty.QuickCheck

import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Normal as N
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import qualified Database.LSMTree.Internal.Run.Construction as Cons
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Util (showPowersOf10)
import           Database.LSMTree.Util.Orphans ()

import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Run"
    [ testGroup "Write buffer to disk"
      [ testCase "Single insert (small)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot "test-key" "test-value" Nothing
      , testCase "Single insert (blob)" $ do
          withSessionDir $ \sessionRoot -> do
            let b = Cons.BlobRef 13 37
            testSingleInsert sessionRoot "test-key" "test-value" (Just b)
      , testCase "Single insert (multi-page)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot
              "test-key"
              ("test-value-" <> BS.concat (replicate 500 "0123456789"))
              Nothing
      , testProperty "Written pages can be read again" $
          forAllShrink genLargeWriteBuffer shrinkWriteBuffer $ \wb ->
            WB.numEntries wb > 0 ==> prop_WriteAndRead wb
      ]
    ]

testSingleInsert :: FilePath -> ByteString -> ByteString -> Maybe Cons.BlobRef -> IO ()
testSingleInsert sessionRoot key val mblob = do
    -- create/write to blob file
    withBinaryFile (sessionRoot </> "active" </> "42.blobs") WriteMode $ \_h ->
      return ()
    -- flush write buffer
    let wb = WB.addEntryNormal key (N.Insert val mblob) WB.emptyWriteBuffer
    _run <- fromWriteBuffer (RunLocation sessionRoot 42) wb
    -- check all files have been written
    bsFilter <- BS.readFile (sessionRoot </> "active" </> "42.filter")
    mempty @=? bsFilter  -- TODO: empty for now, should be written later
    bsIndex <- BS.readFile (sessionRoot </> "active" </> "42.index")
    mempty @=? bsIndex   -- TODO: empty for now, should be written later
    bsKops <- BS.readFile (sessionRoot </> "active" </> "42.keyops")
    -- check page
    let page = makeRawPage (toByteArray bsKops) 0
    1 @=? rawPageNumKeys page
    case mblob of
      Nothing -> do
        0 @=? rawPageNumBlobs page
        Just (Insert (toPrimVec val)) @=? rawPageEntry page (toPrimVec key)
      Just b -> do
        1 @=? rawPageNumBlobs page
        Just (InsertWithBlob (toPrimVec val) (blobRefTuple b)) @=? rawPageEntry page (toPrimVec key)
  where
    toByteArray bs = case SBS.toShort bs of SBS.SBS ba -> BA.ByteArray ba
    toPrimVec bs = V.Vector 0 (BS.length bs) (toByteArray bs)

prop_WriteAndRead :: WriteBuffer ByteString ByteString Cons.BlobRef -> Property
prop_WriteAndRead wb = ioProperty $ withSessionDir $ \sessionRoot -> do
    -- create/write to blob file
    withBinaryFile (sessionRoot </> "active" </> "42.blobs") WriteMode $ \_h ->
      return ()
    -- flush write buffer
    _run <- fromWriteBuffer (RunLocation sessionRoot 42) wb
    -- read pages
    bsKops <- BS.readFile (sessionRoot </> "active" </> "42.keyops")
    let pages = makeRawPage (toByteArray bsKops)
                  <$> [0, 4096 .. (BS.length bsKops - 1)]
    -- check pages
    return $ label ("Number of pages: " <> showPowersOf10 (length pages)) $ do
      let vals = concatMap (bifoldMap pure mempty . snd) (WB.content wb)
      tabulate "Value size" (map (showPowersOf10 . BS.length) vals) $
        pagesContainEntries pages (WB.content wb)
  where
    toByteArray bs = case SBS.toShort bs of SBS.SBS ba -> BA.ByteArray ba

pagesContainEntries :: [RawPage] -> [(ByteString, Entry ByteString Cons.BlobRef)] -> Property
pagesContainEntries [] es = counterexample ("k/ops left: " <> show es) (null es)
pagesContainEntries (page : pages) kops = do
    let keysOnPage = fromIntegral (rawPageNumKeys page)
    let (kopsHere, kopsRest) = splitAt keysOnPage kops
    let valsOnPage = map (rawPageEntry page . toPrimVec . fst) kopsHere
    let valsExpected = map (Just . bimap toPrimVec blobRefTuple . snd) kopsHere
    -- TODO: check blobs?
    let pageBytes = fromIntegral (V.last (rawPageValueOffsets page))
    let overflowPages = (pageBytes - 1) `div` 4096
    classify (overflowPages > 0) "Multi-page value" $
          valsOnPage === valsExpected
     .&&. pagesContainEntries (drop overflowPages pages) kopsRest
  where
    toByteArray bs = case SBS.toShort bs of SBS.SBS ba -> BA.ByteArray ba
    toPrimVec bs = V.Vector 0 (BS.length bs) (toByteArray bs)

blobRefTuple :: Cons.BlobRef -> (Word64, Word32)
blobRefTuple (Cons.BlobRef o s) = (o, s)

withSessionDir :: (FilePath -> IO a) -> IO a
withSessionDir action =
    withTempDirectory "/tmp" "session" $ \sessionRoot -> do
      createDirectoryIfMissing False (sessionRoot </> "active")
      action sessionRoot

-- Large enough to get a few values larger than a page.
genLargeWriteBuffer :: Gen (WriteBuffer ByteString ByteString Cons.BlobRef)
genLargeWriteBuffer = scale (*3) (liftArbitrary2 genVal genBlobRef)
  where
    genBlobRef = Cons.BlobRef <$> arbitrary <*> arbitrary
    genVal = frequency [ (40, arbitrary)
                       , (1,  scale (*40) arbitrary)  -- trigger multi-page
                       ]

shrinkWriteBuffer :: WriteBuffer ByteString ByteString Cons.BlobRef ->
                     [WriteBuffer ByteString ByteString Cons.BlobRef]
shrinkWriteBuffer = liftShrink2 shrink (const [])  -- don't shrink blob refs
