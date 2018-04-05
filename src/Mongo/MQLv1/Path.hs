-- Internal representation for the comma-separated strings that appear in the match, projection, and
-- expression languages. User-facing syntax is something like "employees.id".
module Mongo.MQLv1.Path(
    ImplicitArrayTraversal(..),
    Path(..),
    PathComponent(..),
    PathComponentType(..),

    convertPathToNotTraverseTrailingArrays,
    pathFromStringForTest,
    ) where

import Data.List.Split
import Mongo.Error

-- Each path component represents either the field name of an object, an index of an array, or
-- either (depending on whether the input is an object or array).
data PathComponentType
    = FieldName String
    | ArrayIndex Int
    | FieldNameOrArrayIndex Int
    deriving (Eq, Show)

-- In some contexts in MQL, paths imply specific behavior around arrays. In the match language, for
-- example, suppose we have the expression {"a.b": {$eq: 3}} and that "a" and "b" are both arrays.
-- An example document would be something like {a: [{b: [1, 2]}, {b: [3, 4]}]}. This query matches
-- the example document, since both all path components in the match language (in this case, "a" and
-- "b") imply implicit array traversal behavior.
data ImplicitArrayTraversal = ImplicitlyTraverseArrays | NoImplicitTraversal deriving (Eq, Show)

-- A path component is something like "Field 'foo' in an object, without implicit array traversal"
-- or "Index 3 in an array, with implicit array traversal".
data PathComponent = PathComponent PathComponentType ImplicitArrayTraversal deriving (Eq, Show)

-- A path consists of a sequence of path components. In the user-facing language, '.' characters
-- delimit the components, but we assume no particular syntax here. This is for use in ASTs for the
-- pieces of the language that involve paths.
type Path = [PathComponent]

-- Parses a string to a single path component. See pathFromStringForTest for details on the accepted
-- syntax.
pathComponentFromString :: String -> PathComponent
pathComponentFromString ('$' : '{' : rest) =
    case reverse rest of
        ('}' : reversedField) ->
            PathComponent (FieldName $ reverse reversedField) NoImplicitTraversal
        ('*' : '}' : reversedField) ->
            PathComponent (FieldName $ reverse reversedField) ImplicitlyTraverseArrays
        _ -> error $ "Bad path component: " ++ "${" ++ rest

pathComponentFromString ('$' : '<' : rest) =
    case reverse rest of
        ('>' : reversedField) -> let asInt = read (reverse reversedField) in
            PathComponent (FieldNameOrArrayIndex asInt) NoImplicitTraversal
        ('*' : '>' : reversedField) -> let asInt = read (reverse reversedField) in
            PathComponent (FieldNameOrArrayIndex asInt) ImplicitlyTraverseArrays
        _ -> error $ "Bad path component: " ++ "$<" ++ rest

pathComponentFromString ('$' : '[' : rest) =
    case reverse rest of
        (']' : reversedField) -> let arrIndex = read (reverse reversedField) in
            PathComponent (ArrayIndex arrIndex) NoImplicitTraversal
        ('*' : ']' : reversedField) -> let arrIndex = read (reverse reversedField) in
            PathComponent (ArrayIndex arrIndex) ImplicitlyTraverseArrays
        _ -> error $ "Bad path component: " ++ "$[" ++ rest

pathComponentFromString str =
    case reverse str of
        [] -> error "empty path component"
        ('*' : reversedField) ->
            PathComponent (FieldName $ reverse reversedField) ImplicitlyTraverseArrays
        _ -> PathComponent (FieldName str) NoImplicitTraversal

-- Parses a dot-delimited string to a field path. This accepts a syntax which we define only to make
-- tests more concise:
--  * "f" is used to denote a field in an object, with no implicit traversal.
--  * "$[3]" is used to denote an array index, with no implicit traversal.
--  * "$<4>" denotes the field path that could either be the array index 3 or the field name '3', with no
--    implicit array traversal.
--  * Implicit array traversal is denoted with a "*" character, as in f*, $[3]*, or $<3>*.
--
-- For the purposes of testing, assumes that field names do not contain the characters ".$[]<>*".
--
-- If the input is malformed, throws an exception.
pathFromStringForTest :: String -> Path
pathFromStringForTest str = map pathComponentFromString (splitOn "." str)

-- Modifies the final component of a path to always have NoImplicitTraversal as its array traversal
-- behavior.
--
-- XXX: In some contexts in MQL, the final path component should *never* implicitly traverse arrays.
-- For example, this is true for match expression predicates {$type: "array"} and {$exists: true}.
-- The user should probably have to explicitly encode the "don't traverse trailing arrays" behavior
-- in the path, rather than having the predicate itself encode path traversal behavior.
convertPathToNotTraverseTrailingArrays :: Path -> Path
convertPathToNotTraverseTrailingArrays [] = []
convertPathToNotTraverseTrailingArrays p =
    case last p of (PathComponent t _) -> init p ++ [PathComponent t NoImplicitTraversal]
