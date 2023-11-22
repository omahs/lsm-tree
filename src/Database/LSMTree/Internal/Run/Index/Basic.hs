module Database.LSMTree.Internal.Run.Index.Basic (
    Index
    -- * Queries
  , search
    -- * Construction
  , fromList
  ) where

import qualified Data.List as List
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

-- | A basic fence-pointer indexed, structured as a 'Map'.
newtype Index k = Index (Map k Int)

{-------------------------------------------------------------------------------
  Queries
-------------------------------------------------------------------------------}

search :: Ord k => k -> Index k -> Maybe Int
search k (Index m) = snd <$> Map.lookupLE k m

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

fromList :: Ord k => [(k, k)] -> Index k
fromList []  = Index Map.empty
fromList ks0 = Index . fst $ List.foldl' combine (Map.empty, 0) ks0
  where
    combine (m, nextPageNr) (kmin, _) =
      (Map.insert kmin nextPageNr m, nextPageNr + 1)
