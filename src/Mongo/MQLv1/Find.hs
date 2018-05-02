{-# LANGUAGE LambdaCase #-}

module Mongo.MQLv1.Find(
    Find(..),

    parseFindCommand,
    ) where

import Data.List (sortBy)
import Mongo.Error
import Mongo.MQLv1.MatchExpr
import qualified Text.JSON as JSON

-- Representation for a find command such as {find: "collection", filter: {...}, projection: {...}}.
data Find = Find {
    collection :: String,
    matchExpr :: MatchExpr
    -- TODO: Add support for other find command options, such as 'projection', 'min', and 'max'.
    }

parseFindCommand' :: [(String, JSON.JSValue)] -> Either Error Find
parseFindCommand' [("collection", JSON.JSString str), ("filter", filterVal)] =
    parseMatchExprJson filterVal >>= (\x -> return Find {
            collection = JSON.fromJSString str,
            matchExpr = x })

parseFindCommand' _ = Left Error {
    errCode = FailedToParse,
    errReason = "Illegal arguments to find command" }

parseFindCommand :: JSON.JSValue -> Either Error Find
parseFindCommand (JSON.JSObject obj) =
    parseFindCommand' (sortBy (\(x, _) (y, _) -> x `compare` y) (JSON.fromJSObject obj))
parseFindCommand _ = Left Error {
    errCode = FailedToParse,
    errReason = "Find command must be a document" }
