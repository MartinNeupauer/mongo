{-# LANGUAGE LambdaCase #-}

module Mongo.Database(
    Collection(..),
    DatabaseInstance(..),
    NamespaceString(..),

    collectionToString,
    parseDatabaseInstanceJson,
    parseDatabaseInstanceString,
    ) where

import Mongo.Error
import Mongo.Value
import qualified Text.JSON as JSON

-- A type alias for strings that name collections.
type NamespaceString = String

-- Models a large collection of Values. May be used either to represent user collections stored in a
-- MongoDB deployment, or to represent the result sets of queries (which, like stored collections,
-- are of unbounded size).
newtype Collection = Collection [Value] deriving (Show)

-- Models the complete stage of user data stored in a MongoDB deployment of arbitrary topology.
newtype DatabaseInstance = DatabaseInstance [(NamespaceString, Collection)] deriving (Show)

isObject :: JSON.JSValue -> Bool
isObject (JSON.JSObject _) = True
isObject _ = False

-- Parses a JSON object of the format {namespace: "...", data: [...]} to a (NamespaceString,
-- Collection) pair.
parseCollectionJson :: [(String, JSON.JSValue)] -> Either Error (NamespaceString, Collection)
parseCollectionJson [("namespace", JSON.JSString collName), ("data", JSON.JSArray dataArr)] = do
    parsedValues <- mapM valueFromTextJson dataArr
    return (JSON.fromJSString collName, Collection parsedValues)

parseCollectionJson _ =
    Left Error { errCode = FailedToParse, errReason = "Invalid format for database state" }

-- A database instance consists of one or more collections, represented by a particular JSON format.
-- The input may be either a single object of the format {namespace: "...", data: [...]}, or an
-- array of such objects, as in
--
-- [
--   {namespace: "collection_1", data: [...]},
--   ...
--   {namespace: "collection_n", data: [...]}
-- ]
--
-- Parses either of these JSON formats and returns the corresponding DatabaseInstance, or returns an
-- error if the input JSON does not parse.
parseDatabaseInstanceJson :: JSON.JSValue -> Either Error DatabaseInstance
parseDatabaseInstanceJson (JSON.JSObject v) = do
    coll <- parseCollectionJson (JSON.fromJSObject v)
    return $ DatabaseInstance [coll]

parseDatabaseInstanceJson (JSON.JSArray v) =
    if all isObject v
    then Right . DatabaseInstance
        =<< mapM (parseCollectionJson . (\case (JSON.JSObject obj) -> JSON.fromJSObject obj)) v
    else Left Error { errCode = FailedToParse,
        errReason = "Database state array can only contain nested objects" }

parseDatabaseInstanceJson _ = Left Error {
    errCode = FailedToParse,
    errReason = "Database state must be represented as a JSON object or JSON array" }

-- Like parseDatabaseInstanceJson, but accepts the input as a String rather than a JSValue.
parseDatabaseInstanceString :: String -> Either Error DatabaseInstance
parseDatabaseInstanceString input =
    case JSON.decode input of
        JSON.Ok val -> parseDatabaseInstanceJson val
        JSON.Error jsonErrString -> Left Error {
            errCode = InvalidJSON,
            errReason = "JSON failed to parse: " ++ jsonErrString }

-- Converts a Collection to an Array and returns the extended JSON string representation of this
-- array. The 'mode' argument specifies whether to use canonical or relaxed extended JSON.
collectionToString :: Collection -> ExtendedJsonMode -> String
collectionToString (Collection coll) =
    valueToString (ArrayValue Array { getElements = coll })
