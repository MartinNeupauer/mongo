module Main where

import Mongo.MQLv1.PathTest (pathTest)
import System.Exit (exitFailure, exitSuccess)
import Test.HUnit

-- Import suites of unit tests from test modules, and add them to this list.
allTests :: Test
allTests = TestList [
    pathTest
    ]

main :: IO Counts
main = do
    results <- runTestTT allTests
    if errors results + failures results > 0
    then
        exitFailure
    else
        exitSuccess
