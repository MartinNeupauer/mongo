-- Internal representation for the comma-separated strings that appear in the match, projection, and
-- expression languages. User-facing syntax is something like "employees.id".
module Mongo.MQLv1.Path(
    Path(..),
    PathComponent(..),
    ) where

-- Each path component represents either the field name of an object, an index of an array, or
-- either (depending on whether the input is an object or array).
data PathComponentType
    = FieldName String
    | ArrayIndex Int
    | FieldNameOrArrayIndex String

-- In some contexts in MQL, paths imply specific behavior around arrays. In the match language, for
-- example, suppose we have the expression {"a.b": {$eq: 3}} and that "a" and "b" are both arrays.
-- An example document would be something like {a: [{b: [1, 2]}, {b: [3, 4]}]}. This query matches
-- the example document, since both all path components in the match language (in this case, "a" and
-- "b") imply implicit array traversal behavior.
data ImplicitArrayTraversal = ImplicitlyTraverseArrays | NoImplicitTraversal

-- A path component is something like "Field 'foo' in an object, without implicit array traversal"
-- or "Index 3 in an array, with implicit array traversal".
data PathComponent = PathComponent PathComponentType ImplicitArrayTraversal

-- A path consists of a sequence of path components. In the user-facing language, '.' characters
-- delimit the components, but we assume no particular syntax here. This is for use in ASTs for the
-- pieces of the language that involve paths.
type Path = [PathComponent]
