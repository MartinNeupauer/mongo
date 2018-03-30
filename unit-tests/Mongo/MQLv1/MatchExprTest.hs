module Mongo.MQLv1.MatchExprTest(
    matchExprTest
    ) where

import Mongo.Error
import Mongo.MQLv1.MatchExpr
import Mongo.MQLv1.Path
import Mongo.Value
import Test.HUnit

matchExprTest :: Test
matchExprTest = TestList [
    "equalityMatchEmptyPath" ~: "" ~: Right True ~=?
        evalMatchExpr (EqMatchExpr [] (parseValueOrDie "3")) (parseValueOrDie "3"),

    "equalityMatchDoesNotMatchEmptyPath" ~: "" ~: Right False ~=?
        evalMatchExpr (EqMatchExpr [] (parseValueOrDie "3")) (parseValueOrDie "6"),

    "equalityMatchDescendObjectPath" ~: "" ~: Right True ~=?
        evalMatchExpr (EqMatchExpr (pathFromStringForTest "a.b") (parseValueOrDie "3"))
            (parseValueOrDie "{\"a\": {\"b\": 3}}"),

    "equalityMatchDescendObjectPathRepeatedFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr (EqMatchExpr (pathFromStringForTest "a.a") (parseValueOrDie "3"))
            (parseValueOrDie "{\"a\": {\"a\": 3}}"),

    "equalityMatchDoesNotMatchWhenPathMissing" ~: "" ~: Right False ~=?
        evalMatchExpr (EqMatchExpr (pathFromStringForTest "a.b") (parseValueOrDie "3"))
            (parseValueOrDie "{\"a\": {}}"),

    "equalityConsidersFieldOrder" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr [] (parseValueOrDie "{\"a\": 1, \"b\": 1}"))
            (parseValueOrDie "{\"b\": 1, \"a\": 1}"),

    "traversePathWithArrayIndices" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[1].$[2]") (parseValueOrDie "3"))
            (parseValueOrDie "[0, [1, 2, 3], 4]"),

    "traversePathWithArrayIndicesRepeatedIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[0].$[0]") (parseValueOrDie "3"))
            (parseValueOrDie "[[3]]"),

    "traversePathFieldNameAndArrayIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "b.$[1]") (parseValueOrDie "2"))
            (parseValueOrDie "{\"a\": 0, \"b\": [1, 2, 3], \"c\": 4}"),

    "traversePathFieldNameAndArrayIndexNoMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "b.$[1]") (parseValueOrDie "99"))
            (parseValueOrDie "{\"a\": 0, \"b\": [1, 2, 3], \"c\": 4}"),

    "equalityMatchWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "a*") (parseValueOrDie "2"))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "equalityMatchWithImplicitArrayTraversalNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "a*") (parseValueOrDie "99"))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "arrayDoesNotMatchScalarWithoutImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "a") (parseValueOrDie "2"))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "implicitArrayTraversalMultipleLevels" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "a*.b*") (parseValueOrDie "4"))
            (parseValueOrDie "{\"a\": [{\"b\": [1, 2]}, {\"b\": [3, 4]}]}"),

    "implicitArrayTraversalMultipleLevelsNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "a*.b*") (parseValueOrDie "99"))
            (parseValueOrDie "{\"a\": [{\"b\": [1, 2]}, {\"b\": [3, 4]}]}"),

    "implicitArrayTraversalWithArrayIndexPathComponent" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[0]*") (parseValueOrDie "1"))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[0]*") (parseValueOrDie "99"))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "arrayDoesNotMatchScalarWithoutImplicitTraversalArrayPath" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[0]") (parseValueOrDie "1"))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNested" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$[0]*.$[1]*") (parseValueOrDie "2"))
            (parseValueOrDie "[[[0, []], [1, [2]]]]")
    ]
