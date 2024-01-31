{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as V
import           System.Directory (createDirectoryIfMissing)
import           System.FilePath
import           System.IO
import           System.IO.Temp
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))

import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Normal as N
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.WriteBuffer
import           Database.LSMTree.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Run"
    [ testCase "single-value" $ do
        withSessionDir $ \sessionRoot -> do
          let loc = RunLocation sessionRoot 42
          -- create/write to blob file
          withBinaryFile (runBlobPath loc) WriteMode $ \_h -> return ()
          -- flush write buffer
          let key = "test-key" :: ByteString
          let val = "test-value" :: ByteString
          let wb = addEntryNormal key (N.Insert val Nothing) emptyWriteBuffer
          _run <- fromWriteBuffer loc wb
          -- check all files have been written
          bsFilter <- BS.readFile (sessionRoot </> "active" </> "42.filter")
          mempty @=? bsFilter  -- empty for now, should be written later
          bsIndex <- BS.readFile (sessionRoot </> "active" </> "42.index")
          mempty @=? bsIndex   -- empty for now, should be written later
          bsKops <- BS.readFile (sessionRoot </> "active" </> "42.keyops")
          let page = makeRawPage (toByteArray bsKops) 0
          1 @=? rawPageNumKeys page
          0 @=? rawPageNumBlobs page
          Just (Insert (toPrimVec val)) @=? rawPageEntry page (toPrimVec key)
      -- TODO testCase "larger-than-page"
      -- TODO testCase "multi-page"
    ]
  where
    toByteArray bs = case SBS.toShort bs of SBS.SBS ba -> BA.ByteArray ba
    toPrimVec bs = V.Vector 0 (BS.length bs) (toByteArray bs)

withSessionDir :: (FilePath -> IO a) -> IO a
withSessionDir action =
    withTempDirectory "/tmp" "session" $ \sessionRoot -> do
      createDirectoryIfMissing False (sessionRoot </> "active")
      action sessionRoot
