module Mongo.MQLv1.Find(
    Find(..),

    evalFind,
    parseFindCommandJson,
    parseFindCommandString,
    ) where

import Data.List (sortBy)
import Mongo.CoreStage
import Mongo.Database
import Mongo.Error
import Mongo.MQLv1.MatchExpr
import qualified Text.JSON as JSON

-- Representation for a find command such as {find: "collection", filter: {...}, projection: {...}}.
data Find = Find {
    collection :: NamespaceString,
    matchExpr :: MatchExpr
    -- TODO: Add support for other find command options, such as 'projection', 'min', and 'max'.
    }

parseFindCommand' :: [(String, JSON.JSValue)] -> Either Error Find
parseFindCommand' [("filter", filterVal), ("find", JSON.JSString str)] =
    parseMatchExprJson filterVal >>= (\x -> return Find {
            collection = JSON.fromJSString str,
            matchExpr = x })

parseFindCommand' _ = Left Error {
    errCode = FailedToParse,
    errReason = "Illegal arguments to find command" }

-- Parses a JSObject representing a find command, e.g.
--
--   {find: "collection", filter: <match_expr>, ...}
--
-- Returns an error if the input JSON does not conform to the expected format.
parseFindCommandJson :: JSON.JSValue -> Either Error Find
parseFindCommandJson (JSON.JSObject obj) =
    parseFindCommand' (sortBy (\(x, _) (y, _) -> x `compare` y) (JSON.fromJSObject obj))
parseFindCommandJson _ = Left Error {
    errCode = FailedToParse,
    errReason = "Find command must be a document" }

-- Like parseFindCommandJson, except the input is represented as a string rather than as a JSON
-- value.
parseFindCommandString :: String -> Either Error Find
parseFindCommandString input =
    case JSON.decode input of
        JSON.Ok val -> parseFindCommandJson val
        JSON.Error jsonErrString -> Left Error {
            errCode = InvalidJSON,
            errReason = "JSON failed to parse: " ++ jsonErrString }

-- Find commands are match, project, sort queries. Here we desugar the find command into the
-- corresponding scan -> filter -> transform -> sort core stage tree.
desugarFind :: Find -> CoreStage
desugarFind Find { collection = coll, matchExpr = matchExpr } =
    let desugaredMatch = desugarMatchExpr matchExpr
    in CoreStageFilter desugaredMatch (CoreStageScan coll)

-- Executes a find command against a database instance, and returns either an error or the result
-- set.
evalFind :: Find -> DatabaseInstance -> Either Error Collection
evalFind find = evalCoreStage (desugarFind find)
