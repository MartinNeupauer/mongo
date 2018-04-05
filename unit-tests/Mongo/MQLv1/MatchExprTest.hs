module Mongo.MQLv1.MatchExprTest(
    matchExprTest
    ) where

import Mongo.Error
import Mongo.MQLv1.MatchExpr
import Mongo.MQLv1.Path
import Mongo.Value
import Test.HUnit

-- Tests for basic aspects of MatchExpr path traversal semantics, using an equality match
-- expression.
basicTests :: Test
basicTests = TestList [
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
            (parseValueOrDie "[[[0, []], [1, [2]]]]"),

    "fieldNameOrIndexCanActAsIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>") (parseValueOrDie "1"))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsIndexValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>") (parseValueOrDie "99"))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>") (parseValueOrDie "3"))
            (parseValueOrDie "{\"1\": 3}"),

    "repeatedFieldNameOrIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<0>.$<0>") (parseValueOrDie "3"))
            (parseValueOrDie "{\"0\": {\"0\": 3}}"),

    "fieldNameOrIndexCanActAsFieldNameValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>") (parseValueOrDie "99"))
            (parseValueOrDie "{\"1\": 3}"),

    "fieldNameOrIndexActsAsFieldNameWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>*") (parseValueOrDie "3"))
            (parseValueOrDie "{\"1\": [1, 2, 3]}"),

    -- XXX: This is an odd special case in MQLv1.
    "fieldNameOrIndexDoesNotUseImplicitTraversalWhenActsAsIndex" ~: "" ~: Right False ~=?
        evalMatchExpr
            (EqMatchExpr (pathFromStringForTest "$<1>*") (parseValueOrDie "3"))
            (parseValueOrDie "[0, [1, 2, 3]]")
    ]

-- Tests for $lt, $lte, $gt, and $gte.
inequalityTests :: Test
inequalityTests = TestList [
    "ltMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (LTMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "-1"),

    "ltDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (LTMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "1"),

    "ltMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (LTMatchExpr (pathFromStringForTest "foo*") (parseValueOrDie "0"))
        (parseValueOrDie "{\"foo\": [1, 2, -1]}"),

    "lteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (LTEMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "0"),

    "lteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (LTEMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "2"),

    "lteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (LTEMatchExpr (pathFromStringForTest "foo*") (parseValueOrDie "0"))
        (parseValueOrDie "{\"foo\": [1, 2, -1]}"),

    "gtMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (GTMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "1"),

    "gtDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (GTMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "-1"),

    "gtMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (GTMatchExpr (pathFromStringForTest "foo*") (parseValueOrDie "0"))
        (parseValueOrDie "{\"foo\": [0, 2, -1]}"),

    "gteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (GTEMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "0"),

    "gteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (GTEMatchExpr [] (parseValueOrDie "0")) (parseValueOrDie "-1"),

    "gteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (GTMatchExpr (pathFromStringForTest "foo*") (parseValueOrDie "0"))
        (parseValueOrDie "{\"foo\": [-3, 2, -1]}")
    ]

-- Tests for $exists:true and $exists:false.
existsTests :: Test
existsTests = TestList [
    "existsMatchesIntWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr []) (parseValueOrDie "3"),

    "existsMatchesNullWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr []) (parseValueOrDie "null"),

    "existsMatchesUndefinedWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr []) UndefinedValue,

    "existsMatchesEmptyArrayWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr []) (parseValueOrDie "[]"),

    "existsDoesNotMatchWhenPathDoesntExist" ~: "" ~: Right False ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "a.b") (parseValueOrDie "{\"a\": 1}"),

    "existsDoesNotMatchWhenPathDoesntExistWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "a*.b*")
            (parseValueOrDie "{\"a\": [1, {\"c\": 2}]}"),

    "existsMatchesWhenPathExistsWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "a*.b*")
            (parseValueOrDie "{\"a\": [1, {\"b\": 2}]}"),

    "existsMatchesWhenPathExistsWithImplicitTraversalTrailingEmptyArray" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "a*.b*")
            (parseValueOrDie "{\"a\": [1, {\"b\": []}]}"),

    "existsMatchesWhenPathExistsWithImplicitTraversalArrIndexPath" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "$<1>*.$<1>*")
            (parseValueOrDie "[0, [1, []], 2]"),

    "existsMatchesArrIndexResolvesToFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "$<1>*.$<1>*")
            (parseValueOrDie "{\"1\": {\"1\": []}}"),

    "existsDoesNotMatchWhenArrIndexToHigh" ~: "" ~: Right False ~=?
        evalMatchExpr (ExistsExpr $ pathFromStringForTest "$<8>*")
            (parseValueOrDie "[1, 2, 3]"),

    "notExistsDoesNotMatchInt" ~: "" ~: Right False ~=?
        evalMatchExpr (NotExpr (ExistsExpr [])) (parseValueOrDie "3"),

    "notExistsDoesNotMatchNull" ~: "" ~: Right False ~=?
        evalMatchExpr (NotExpr (ExistsExpr [])) (parseValueOrDie "null"),

    "notExistsDoesNotMatchUndefined" ~: "" ~: Right False ~=?
        evalMatchExpr (NotExpr (ExistsExpr [])) (parseValueOrDie "undefined"),

    "notExistsDoesNotMatchWhenObjectPathExists" ~: "" ~: Right False ~=?
        evalMatchExpr (NotExpr (ExistsExpr $ pathFromStringForTest "a.b"))
            (parseValueOrDie "{\"a\": {\"b\": 1}}"),

    "notExistsMatchesWhenObjectPathDoesNotExist" ~: "" ~: Right True ~=?
        evalMatchExpr (NotExpr (ExistsExpr $ pathFromStringForTest "a.b"))
            (parseValueOrDie "{\"a\": {\"c\": 1}}"),

    "notExistsDoesNotMatchEmptyArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr (NotExpr (ExistsExpr $ pathFromStringForTest "a*"))
            (parseValueOrDie "{\"a\": []}")
    ]

matchExprTest :: Test
matchExprTest = TestList [
    basicTests,
    existsTests,
    inequalityTests
    ]
