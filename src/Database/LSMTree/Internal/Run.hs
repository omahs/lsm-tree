{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Functionality related to LSM-Tree runs (sequences of LSM-Tree data).
--
-- === TODO
--
-- This is temporary module header documentation. The module will be
-- fleshed out more as we implement bits of it.
--
-- Related work packages: 5, 6
--
-- This module includes in-memory parts and I\/O parts for, amongst others,
--
-- * High-performance batch lookups
--
-- * Range lookups
--
-- * Incremental run construction
--
-- * Lookups in loaded disk pages, value resolution
--
-- * In-memory representation of a run
--
-- * Flushing a write buffer to a run
--
-- * Opening, deserialising, and verifying files for an LSM run.
--
-- * Closing runs (and removing files)
--
-- * high performance, incremental k-way merge
--
-- The above list is a sketch. Functionality may move around, and the list is
-- not exhaustive.
--
module Database.LSMTree.Internal.Run (
    RunLocation (..)
  , runKOpsPath
  , runFilterPath
  , runIndexPath
  , runBlobPath

  , Run
  , fromWriteBuffer
  ) where

import qualified Control.Monad.ST as ST
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Short as BS
import           Data.Foldable (for_)
import           Database.LSMTree.Common (SomeSerialisationConstraint (..))
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom)
import           Database.LSMTree.Internal.Run.Construction (BlobRef, RawValue)
import qualified Database.LSMTree.Internal.Run.Construction as Cons
import           Database.LSMTree.Internal.Run.Index.Compact (CompactIndex)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import qualified Database.LSMTree.Internal.Serialise as S
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           System.FilePath ((</>))
import qualified System.IO as IO

-- TODO: makes sense? in any case, it should live somewhere else
data RunLocation = RunLocation
    { runSessionRoot :: !FilePath
    , runNumber      :: !Int
    }

runKOpsPath :: RunLocation -> FilePath
runKOpsPath = runFilePathWithExt ".keyops"

runFilterPath :: RunLocation -> FilePath
runFilterPath = runFilePathWithExt ".filter"

runIndexPath :: RunLocation -> FilePath
runIndexPath = runFilePathWithExt ".index"

runBlobPath :: RunLocation -> FilePath
runBlobPath = runFilePathWithExt ".blobs"

runFilePathWithExt :: String -> RunLocation -> FilePath
runFilePathWithExt ext RunLocation {..} =
    runSessionRoot </> "active" </> (show runNumber <> ext)

-- Long term we want to work with FsPath
-- runFilePathWithExt :: Strict.Text -> RunLocation -> FsPath
-- runFilePathWithExt ext RunLocation{..} =
--   fsPathFromList $
--     fsPathToList runSessionRoot <> ["active", Strict.pack (show runNumber) <> ext]

--------------------------------------------------------------------------------

-- TODO: what do we want the representation to be like?
data Run h = Run
    { runLocation :: !RunLocation
    , runFilter   :: !(Bloom S.SerialisedKey)
    , runIndex    :: !CompactIndex
    , runKopsFile :: !h
    , runBlobFile :: !h
    }

fromWriteBuffer :: forall k v.
                   (S.Serialise k, SomeSerialisationConstraint v) =>
                   RunLocation -> WriteBuffer k v BlobRef -> IO (Run IO.Handle)
fromWriteBuffer fp buffer =
    fromSortedKOps fp numEntries estimatedNumPages (map toKOp (WB.content buffer))
  where
    numEntries = WB.numEntries buffer
    -- Just a rough estimate, we need information about key and value size
    estimatedNumPages = numEntries `div` (4096 `div` 100) + 1

toKOp :: (S.Serialise k, SomeSerialisationConstraint v) =>
         (k, Entry v BlobRef) -> (SerialisedKey, Entry RawValue BlobRef)
toKOp (k, e) = (S.serialise k, encodeOperation e)

encodeOperation :: SomeSerialisationConstraint v =>
                   Entry v BlobRef -> Entry RawValue BlobRef
encodeOperation = \case
    Insert v -> Insert (serialiseValue v)
    InsertWithBlob v ref -> InsertWithBlob (serialiseValue v) ref
    Mupdate v -> Mupdate (serialiseValue v)
    Delete -> Delete

serialiseValue :: SomeSerialisationConstraint v => v -> RawValue
serialiseValue = Cons.rawValueFromShortByteString . BS.toShort . serialise


fromSortedKOps :: RunLocation -> Int -> Int -> [(SerialisedKey, Entry RawValue BlobRef)] -> IO (Run IO.Handle)
fromSortedKOps runLocation numEntries estimatedNumPages = \kops -> do
    -- TODO: handle exceptions?
    (runFilter, runIndex) <-
      IO.withBinaryFile (runKOpsPath runLocation) IO.WriteMode $ \h -> do
        run <- ST.stToIO (Cons.new numEntries estimatedNumPages)
        writeKOps h run kops

    IO.withBinaryFile (runFilterPath runLocation) IO.WriteMode $ \_h -> do
      return ()  -- TODO: write filter
    IO.withBinaryFile (runIndexPath runLocation) IO.WriteMode $ \_h -> do
      return ()  -- TODO: write index

    runKopsFile <- IO.openBinaryFile (runKOpsPath runLocation) IO.ReadMode
    runBlobFile <- IO.openBinaryFile (runBlobPath runLocation) IO.ReadMode
    return Run {..}

writeKOps :: IO.Handle -> Cons.MRun ST.RealWorld -> [(SerialisedKey, Entry RawValue BlobRef)] ->
             IO (Bloom SerialisedKey, CompactIndex)
writeKOps fd run = go
  where
    go [] = do
      (mAcc, _, _, runFilter, runIndex) <- ST.stToIO (Cons.unsafeFinalise run)
      for_ mAcc $ \(pageAcc, _) ->
        BS.hPutBuilder fd (Cons.pageBuilder pageAcc)
      return (runFilter, runIndex)
    go ((key, op) : kops) = do
      ST.stToIO (Cons.addFullKOp run key op) >>= \case
        Nothing           -> return ()
        Just (pageAcc, _) -> BS.hPutBuilder fd (Cons.pageBuilder pageAcc)
      go kops
