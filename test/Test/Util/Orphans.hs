{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Util.Orphans () where

import           Control.Concurrent.Class.MonadMVar (MonadMVar (..))
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import qualified Control.Concurrent.MVar as Real
import qualified Control.Concurrent.STM as Real
import           Control.Monad ((<=<))
import           Control.Monad.IOSim (IOSim)
import           Data.Bifunctor (first)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Kind (Type)
import qualified Data.Map as Map
import           Database.LSMTree.Common
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise (Serialise)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer (..))
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Monoidal as Monoidal
import           Database.LSMTree.Normal (LookupResult, RangeLookupResult,
                     TableHandle)
import qualified Database.LSMTree.Normal as Normal
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (..), Arbitrary1 (..),
                     Arbitrary2 (..), frequency, oneof)
import           Test.QuickCheck.Instances ()
import           Test.QuickCheck.Modifiers
import           Test.QuickCheck.StateModel (Realized)
import           Test.QuickCheck.StateModel.Lockstep (InterpretOp)
import qualified Test.QuickCheck.StateModel.Lockstep.Op as Op
import qualified Test.QuickCheck.StateModel.Lockstep.Op.SumProd as SumProd
import           Test.Util.TypeFamilyWrappers (WrapBlob (..), WrapBlobRef (..),
                     WrapTableHandle (..))

{-------------------------------------------------------------------------------
  Common LSMTree types
-------------------------------------------------------------------------------}

instance (Arbitrary v, Arbitrary blob) => Arbitrary (Normal.Update v blob) where
  arbitrary = QC.arbitrary2
  shrink = QC.shrink2

instance Arbitrary2 Normal.Update where
  liftArbitrary2 genVal genBlob = frequency
    [ (10, Normal.Insert <$> genVal <*> oneof [pure Nothing, Just <$> genBlob])
    , (1, pure Normal.Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = \case
    Normal.Insert v blob -> Normal.Delete : map (uncurry Normal.Insert) (liftShrink2 shrinkVal (liftShrink shrinkBlob) (v, blob))
    Normal.Delete        -> []

instance (Arbitrary v) => Arbitrary (Monoidal.Update v) where
  arbitrary = QC.arbitrary1
  shrink = QC.shrink1

instance Arbitrary1 Monoidal.Update where
  liftArbitrary genVal = frequency
    [ (10, Monoidal.Insert <$> genVal)
    , (5, Monoidal.Mupsert <$> genVal)
    , (1, pure Monoidal.Delete)
    ]

  liftShrink shrinkVal = \case
    Monoidal.Insert v  -> Monoidal.Delete : map Monoidal.Insert (shrinkVal v)
    Monoidal.Mupsert v -> Monoidal.Insert v : map Monoidal.Mupsert (shrinkVal v)
    Monoidal.Delete    -> []

instance Arbitrary k => Arbitrary (Range k) where
  arbitrary = oneof
    [ FromToExcluding <$> arbitrary <*> arbitrary
    , FromToIncluding <$> arbitrary <*> arbitrary
    ]

instance Arbitrary2 Entry where
  liftArbitrary2 genVal genBlob = frequency
    [ (10, Insert <$> genVal)
    , (1,  InsertWithBlob <$> genVal <*> genBlob)
    , (1,  Mupdate <$> genVal)
    , (1,  pure Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = \case
    Insert v           -> Delete : (Insert <$> shrinkVal v)
    InsertWithBlob v b -> [Delete, Insert v]
                       ++ [ InsertWithBlob v' b'
                          | (v', b') <- liftShrink2 shrinkVal shrinkBlob (v, b)
                          ]
    Mupdate v          -> Delete : Insert v : (Mupdate <$> shrinkVal v)
    Delete             -> []

-- | Assuming that keys are bytestrings gives us control over their length.
--
instance Arbitrary2 (WriteBuffer ByteString) where
  liftArbitrary2 genVal genBlob = do
    keyLen <- QC.chooseInt (4, 128)  -- TODO: which conditions on keys?
    let genKey = BS.pack <$> QC.vector keyLen
    let genEntry = liftArbitrary2 genVal genBlob
    WB . Map.fromList <$> QC.listOf (liftArbitrary2 genKey genEntry)

  liftShrink2 shrinkVal shrinkBlob wb =
      -- decrease key length, makes output much more readable
      [ (WB . Map.fromList) (map (first (BS.take keyLen')) kops)
      | (firstKey, _) : _ <- [kops]
      , keyLen' <- [4..(BS.length firstKey - 1)]
      ]
      -- shrink everything else
   ++ map (WB . Map.fromList)
      (liftShrink (liftShrink2 mempty (liftShrink2 shrinkVal shrinkBlob)) kops)
    where
      kops = WB.content wb



{-------------------------------------------------------------------------------
  IOSim
-------------------------------------------------------------------------------}

instance IOLike (IOSim s)

type instance Realized (IOSim s) a = RealizeIOSim s a

type RealizeIOSim :: Type -> Type -> Type
type family RealizeIOSim s a where
  -- io-classes
  RealizeIOSim s (Real.TVar a)  = TVar (IOSim s) a
  RealizeIOSim s (Real.TMVar a) = TMVar (IOSim s) a
  RealizeIOSim s (Real.MVar a)  = MVar (IOSim s) a
  -- lsm-tree
  RealizeIOSim s (TableHandle IO k v blob)       = TableHandle (IOSim s) k v blob
  RealizeIOSim s (LookupResult k v blobref)      = LookupResult k v blobref
  RealizeIOSim s (RangeLookupResult k v blobref) = RangeLookupResult k v blobref
  RealizeIOSim s (BlobRef blob)                  = BlobRef blob
  -- Type family wrappers
  RealizeIOSim s (WrapTableHandle h IO k v blob) = WrapTableHandle h (IOSim s) k v blob
  RealizeIOSim s (WrapBlobRef h blob)            = WrapBlobRef h blob
  RealizeIOSim s (WrapBlob blob)                 = WrapBlob blob
  -- Congruence
  RealizeIOSim s (f a b) = f (RealizeIOSim s a) (RealizeIOSim s b)
  RealizeIOSim s (f a)   = f (RealizeIOSim s a)
  -- Default
  RealizeIOSim s a = a

instance InterpretOp SumProd.Op (Op.WrapRealized (IOSim s)) where
  intOp ::
       SumProd.Op a b
    -> Op.WrapRealized (IOSim s) a
    -> Maybe (Op.WrapRealized (IOSim s) b)
  intOp = \case
      SumProd.OpId    -> Just
      SumProd.OpFst   -> Just . Op.WrapRealized . fst . Op.unwrapRealized
      SumProd.OpSnd   -> Just . Op.WrapRealized . snd . Op.unwrapRealized
      SumProd.OpLeft  -> either (Just . Op.WrapRealized) (const Nothing) . Op.unwrapRealized
      SumProd.OpRight -> either (const Nothing) (Just . Op.WrapRealized) . Op.unwrapRealized
      SumProd.OpComp g f -> Op.intOp g <=< Op.intOp f

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

deriving newtype instance Serialise a => Serialise (Small a)
