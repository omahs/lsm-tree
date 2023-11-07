module Main (main) where

import qualified Test.Database.LSMTree.Common
import qualified Test.Database.LSMTree.Model.Monoidal
import qualified Test.Database.LSMTree.Model.Normal
import qualified Test.Database.LSMTree.ModelIO.Monoidal
import qualified Test.Database.LSMTree.ModelIO.Normal
import qualified Test.Database.LSMTree.Normal.StateMachine
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "lsm-tree"
    [ Test.Database.LSMTree.Common.tests
    , Test.Database.LSMTree.Model.Normal.tests
    , Test.Database.LSMTree.Model.Monoidal.tests
    , Test.Database.LSMTree.ModelIO.Normal.tests
    , Test.Database.LSMTree.ModelIO.Monoidal.tests
    , Test.Database.LSMTree.Normal.StateMachine.tests
    ]
