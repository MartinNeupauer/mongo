{-# LANGUAGE LambdaCase #-}

module Mongo.Main (
    mongoMain) where

import Mongo.MQLv1.MatchExpr
import Mongo.CoreExpr
import Mongo.ParseCoreExpr
import Mongo.Error
import Mongo.Value
import qualified Options.Applicative as OA
import Data.Semigroup ((<>))
import System.IO
import System.Exit (exitFailure, exitSuccess)

-- Monad transformer for combining IO with Either
newtype EitherT m e a = EitherT { runEitherT :: m (Either e a) }
instance Monad m => Functor (EitherT m e) where
    fmap f = EitherT . fmap (fmap f) . runEitherT

instance Monad m => Applicative (EitherT m e) where
    pure a = EitherT $ pure (Right a)
    f <*> a = EitherT $ (<*>) <$> runEitherT f <*> runEitherT a

instance Monad m => Monad (EitherT m e) where
    return a = EitherT $ return (Right a)

    x >>= f = EitherT $ runEitherT x >>= \case
        Right value -> runEitherT $ f value
        Left e -> return $ Left e

lift :: Applicative m => Either e a -> EitherT m e a
lift x = EitherT $ pure x

-- Command line options
data CmdLineOptions
    = MatchQueryOption
        {
            matchQueryFile :: String,
            dataFile :: String
        }
    | MatchDesugarOption
        {
            matchQueryFile :: String
        }
    | TestModeOption
        {
            testsFile :: String
        }

-- Parsing of command line options
matchQueryOption :: OA.Parser CmdLineOptions
matchQueryOption = MatchQueryOption
            <$> OA.strOption
             ( OA.long "match"
            <> OA.metavar "FILE"
            <> OA.help "Evaluate a match query")
            <*> OA.strOption
            ( OA.long "data"
            <> OA.metavar "FILE"
            <> OA.help "Input to a query")

matchDesugarOption :: OA.Parser CmdLineOptions
matchDesugarOption = MatchDesugarOption
            <$> OA.strOption
             ( OA.long "desugar-match"
            <> OA.metavar "FILE"
            <> OA.help "Desugar a match query into a CoreExpr expression")

testModeOption :: OA.Parser CmdLineOptions
testModeOption = TestModeOption
            <$> OA.strOption
             ( OA.long "test"
            <> OA.metavar "FILE"
            <> OA.help "Run tests as described in an input file")

options :: OA.Parser CmdLineOptions
options = matchDesugarOption OA.<|> matchQueryOption OA.<|> testModeOption

opts :: OA.ParserInfo CmdLineOptions
opts = OA.info (options OA.<**> OA.helper)
  ( OA.fullDesc
  <> OA.progDesc "mql model runner"
  <> OA.header "mql -- A model implementation of MongoDB Query Language." )

-- Evaluate a single match expression
--   query is a string that gets parsed into a match expression
--   input is a string that gets parsed into a Value 
-- The function returns Error if a parsing fails or an evaluation fails
-- otherwise it returns a boolean (match/non match)
runMatchQuery :: String -> String -> Either Error Bool
runMatchQuery query input =
    do
        matchExpr <- parseMatchExprString query
        value <- valueFromString input

        evalMatchExpr matchExpr value

desugarMatchQuery :: String -> Either Error (CoreExpr Bool)
desugarMatchQuery q =
    desugarMatchExpr <$> parseMatchExprString q

-- Run a signle test. The test description is a JSON object
-- {"match": "filename", "data": "filename", "expected": bool}
-- The function returns Error if a parsing fails or an evaluation fails
-- otherwise it returns a boolean True <=> result == expected
runOneTest :: Value -> EitherT IO Error Bool
runOneTest test =
    do
        testDescription <- lift $ getDocumentValue test
        dataFile <- lift $ getField "data" testDescription >>= getStringValue
        expected <- lift $ getField "expected" testDescription

        if hasField "match" testDescription
        then
            do
                queryFile <- lift $ getField "match" testDescription >>= getStringValue
                result <- EitherT $ do
                                        putStr $ "Running test: " ++ queryFile ++ " ... "
                                        query <- openFile queryFile ReadMode >>= hGetContents
                                        input <- openFile dataFile ReadMode >>= hGetContents
                                        return $ runMatchQuery query input

                let testPassed = BoolValue result == expected
                    in
                        EitherT $ do
                                        putStrLn (if testPassed then "OK" else "Failed")
                                        return $ Right testPassed
        else
            lift $ Left Error {
                errCode = FailedToParse,
                errReason = "Unknown test descprion: " ++ show (valueToString test Canonical) }

-- Run either a single test or a set of tests.
runTests :: String -> EitherT IO Error Bool
runTests tests =
    do
        testsAsValue <- lift (valueFromString tests)
        if isArray testsAsValue
        then
            do
                v <- lift (getArrayValue testsAsValue)
                results <- traverse runOneTest (getElements v)
                return $ and results
        else
            runOneTest testsAsValue

run :: CmdLineOptions -> IO ()
run (MatchQueryOption queryFile dataFile) =
    do
        query <- openFile queryFile ReadMode >>= hGetContents
        input <- openFile dataFile ReadMode >>= hGetContents

        case runMatchQuery query input of
            (Left e) -> putStrLn (errorToString e) >> exitFailure
            (Right v) -> print v

run (MatchDesugarOption queryFile) =
    do
        query <- openFile queryFile ReadMode >>= hGetContents

        case desugarMatchQuery query of
            (Left e) -> putStrLn (errorToString e) >> exitFailure
            (Right v) -> putStrLn $ valueToString (coreExprToValue v) Canonical

run (TestModeOption f) =
    do
        tests <- openFile f ReadMode >>= hGetContents
        result <- runEitherT $ runTests tests
        case result of
            (Left e) -> putStrLn (errorToString e) >> exitFailure
            (Right v) -> if v then exitSuccess else exitFailure

mongoMain :: IO ()
mongoMain = run =<< OA.execParser opts
