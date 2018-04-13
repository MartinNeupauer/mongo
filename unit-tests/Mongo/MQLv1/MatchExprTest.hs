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
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "3"))) (parseValueOrDie "3"),

    "equalityMatchDoesNotMatchEmptyPath" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "3"))) (parseValueOrDie "6"),

    "equalityMatchDescendObjectPath" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"a\": {\"b\": 3}}"),

    "equalityMatchDescendObjectPathRepeatedFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.a") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"a\": {\"a\": 3}}"),

    "equalityMatchDoesNotMatchWhenPathMissing" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"a\": {}}"),

    "equalityConsidersFieldOrder" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "{\"a\": 1, \"b\": 1}")))
            (parseValueOrDie "{\"b\": 1, \"a\": 1}"),

    "traversePathWithArrayIndices" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[1].$[2]")
                (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "[0, [1, 2, 3], 4]"),

    "traversePathWithArrayIndicesRepeatedIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0].$[0]")
                (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "[[3]]"),

    "traversePathFieldNameAndArrayIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "b.$[1]") (EqMatchExpr (parseValueOrDie "2")))
            (parseValueOrDie "{\"a\": 0, \"b\": [1, 2, 3], \"c\": 4}"),

    "traversePathFieldNameAndArrayIndexNoMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "b.$[1]")
                (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "{\"a\": 0, \"b\": [1, 2, 3], \"c\": 4}"),

    "equalityMatchWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "2")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "equalityMatchWithImplicitArrayTraversalNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "arrayDoesNotMatchScalarWithoutImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (EqMatchExpr (parseValueOrDie "2")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "implicitArrayTraversalMultipleLevels" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") (EqMatchExpr (parseValueOrDie "4")))
            (parseValueOrDie "{\"a\": [{\"b\": [1, 2]}, {\"b\": [3, 4]}]}"),

    "implicitArrayTraversalMultipleLevelsNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "{\"a\": [{\"b\": [1, 2]}, {\"b\": [3, 4]}]}"),

    "implicitArrayTraversalWithArrayIndexPathComponent" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*") (EqMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*") (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "arrayDoesNotMatchScalarWithoutImplicitTraversalArrayPath" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]") (EqMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNested" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*.$[1]*")
                (EqMatchExpr (parseValueOrDie "2")))
            (parseValueOrDie "[[[0, []], [1, [2]]]]"),

    "fieldNameOrIndexCanActAsIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsIndexValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"1\": 3}"),

    "repeatedFieldNameOrIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<0>.$<0>")
                (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"0\": {\"0\": 3}}"),

    "fieldNameOrIndexCanActAsFieldNameValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchExpr (parseValueOrDie "99")))
            (parseValueOrDie "{\"1\": 3}"),

    "fieldNameOrIndexActsAsFieldNameWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "{\"1\": [1, 2, 3]}"),

    -- XXX: This is an odd special case in MQLv1.
    "fieldNameOrIndexDoesNotUseImplicitTraversalWhenActsAsIndex" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*") (EqMatchExpr (parseValueOrDie "3")))
            (parseValueOrDie "[0, [1, 2, 3]]")
    ]

-- Tests for $lt, $lte, $gt, $gte, and $ne.
inequalityTests :: Test
inequalityTests = TestList [
    "ltMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (LTMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "ltDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (LTMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "1"),

    "ltMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (LTMatchExpr (parseValueOrDie "0")))
        (parseValueOrDie "{\"foo\": [1, 2, -1]}"),

    "lteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (LTEMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "0"),

    "lteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (LTEMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "2"),

    "lteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "foo*") (LTEMatchExpr (parseValueOrDie "0")))
            (parseValueOrDie "{\"foo\": [1, 2, -1]}"),

    "gtMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (GTMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "1"),

    "gtDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (GTMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "gtMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (GTMatchExpr (parseValueOrDie "0")))
        (parseValueOrDie "{\"foo\": [0, 2, -1]}"),

    "gteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (GTEMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "0"),

    "gteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (GTEMatchExpr (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "gteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (GTEMatchExpr (parseValueOrDie "0")))
        (parseValueOrDie "{\"foo\": [-3, 2, -1]}"),

    "neMatchesUnequalInt" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (NEMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "2"),

    "neDoesntMatchEqualInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (NEMatchExpr (parseValueOrDie "2")))
            (parseValueOrDie "2"),

    "neMatchesArrayWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (NEMatchExpr (parseValueOrDie "5")))
            (parseValueOrDie "{\"a\": [3, 4]}"),

    "neDoesntMatchArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (NEMatchExpr (parseValueOrDie "4")))
            (parseValueOrDie "{\"a\": [3, 4]}"),

    "arrayNEArrayWithImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*") (NEMatchExpr (parseValueOrDie "[3, 4]")))
            (parseValueOrDie "{\"a\": [3, 4]}"),

    "arrayNEArrayWithImplicitTraversalDoesntMatchNested" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*") (NEMatchExpr (parseValueOrDie "[3, 4]")))
            (parseValueOrDie "{\"a\": [0, [3, 4]]}"),

    "arrayNEArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*") (NEMatchExpr (parseValueOrDie "[3, 4]")))
            (parseValueOrDie "{\"a\": [3, 5]}"),

    "arrayNEArrayWithoutImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (NEMatchExpr (parseValueOrDie "[3, 4]")))
            (parseValueOrDie "{\"a\": [0, [3, 4]]}")
    ]

-- Tests for $exists:true and $exists:false.
existsTests :: Test
existsTests = TestList [
    "existsMatchesIntWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchExpr) (parseValueOrDie "3"),

    "existsMatchesNullWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchExpr) (parseValueOrDie "null"),

    "existsMatchesUndefinedWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchExpr) UndefinedValue,

    "existsMatchesEmptyArrayWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchExpr) (parseValueOrDie "[]"),

    "existsDoesNotMatchWhenPathDoesntExist" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchExpr)
            (parseValueOrDie "{\"a\": 1}"),

    "existsDoesNotMatchWhenPathDoesntExistWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchExpr)
            (parseValueOrDie "{\"a\": [1, {\"c\": 2}]}"),

    "existsMatchesWhenPathExistsMatchExprWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchExpr)
            (parseValueOrDie "{\"a\": [1, {\"b\": 2}]}"),

    "existsMatchesWhenPathExistsMatchExprWithImplicitTraversalTrailingEmptyArray" ~: "" ~:
        Right True ~=?
            evalMatchExpr
                (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchExpr)
                (parseValueOrDie "{\"a\": [1, {\"b\": []}]}"),

    "existsMatchesWhenPathExistsMatchExprWithImplicitTraversalArrIndexPath" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*.$<1>*") ExistsMatchExpr)
            (parseValueOrDie "[0, [1, []], 2]"),

    "existsMatchesArrIndexResolvesToFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*.$<1>*") ExistsMatchExpr)
            (parseValueOrDie "{\"1\": {\"1\": []}}"),

    "existsDoesNotMatchWhenArrIndexToHigh" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<8>*") ExistsMatchExpr)
            (parseValueOrDie "[1, 2, 3]"),

    "notExistsMatchExprDoesNotMatchInt" ~: "" ~: Right False ~=?
        evalMatchExpr (NotMatchExpr (PathAcceptingExpr [] ExistsMatchExpr)) (parseValueOrDie "3"),

    "notExistsMatchExprDoesNotMatchNull" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr [] ExistsMatchExpr)) (parseValueOrDie "null"),

    "notExistsMatchExprDoesNotMatchUndefined" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr [] ExistsMatchExpr)) (parseValueOrDie "undefined"),

    "notExistsMatchExprDoesNotMatchWhenObjectPathExistsMatchExpr" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchExpr))
            (parseValueOrDie "{\"a\": {\"b\": 1}}"),

    "notExistsMatchExprMatchesWhenObjectPathDoesNotExist" ~: "" ~: Right True ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchExpr))
            (parseValueOrDie "{\"a\": {\"c\": 1}}"),

    "notExistsMatchExprDoesNotMatchEmptyArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a*") ExistsMatchExpr))
            (parseValueOrDie "{\"a\": []}")
    ]

-- Tests for $and, $or, $not, and $nor.
logicalExprTests :: Test
logicalExprTests = TestList [
    "emptyAndMatchesInt" ~: "" ~: Right True ~=?
        evalMatchExpr (AndMatchExpr []) (parseValueOrDie "1"),

    "andOfOneThingIsTheThingMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (AndMatchExpr [PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "1"))])
            (parseValueOrDie "1"),

    "andOfOneThingIsTheThingDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (AndMatchExpr [PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "99"))])
            (parseValueOrDie "1"),

    "andOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (AndMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "2")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "3"))])
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}"),

    "andOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (AndMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "2")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "99")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "3"))])
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}"),

    "emptyOrDoesntMatchInt" ~: "" ~: Right False ~=?
        evalMatchExpr (OrMatchExpr []) (parseValueOrDie "1"),

    "orOfOneThingIsTheThingMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (OrMatchExpr [PathAcceptingExpr[] (EqMatchExpr (parseValueOrDie "1"))])
            (parseValueOrDie "1"),

    "orOfOneThingIsTheThingDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (OrMatchExpr [PathAcceptingExpr[] (EqMatchExpr (parseValueOrDie "99"))])
            (parseValueOrDie "1"),

    "orOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "9"))])
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}"),

    "orOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "7")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "9"))])
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}"),

    "emptyNorMatchesInt" ~: "" ~: Right True ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [])) (parseValueOrDie "1"),

    "norOfOneThingIsNotMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (NotMatchExpr (OrMatchExpr [PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "99"))]))
            (parseValueOrDie "1"),

    "norOfOneThingIsNotDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (OrMatchExpr [PathAcceptingExpr [] (EqMatchExpr (parseValueOrDie "1"))]))
            (parseValueOrDie "1"),

    "norOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "7")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "9"))]))
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}"),

    "norOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchExpr (parseValueOrDie "9"))]))
                (parseValueOrDie "{\"a\": [0, 1, 2, 3]}")
    ]

comparisonToArrayTests :: Test
comparisonToArrayTests = TestList [
    "eqArrayWithoutImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (EqMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "eqArrayWithoutImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (EqMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2, 99]}"),

    "eqWholeArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "eqNestedArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [0, [1, 2, 3]]}"),

    "eqNestedArrayWithImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [0, [1, 2, 3, 4]]}"),

    "ltArrayMatchesWhenFirstEltIsSmaller" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [0, 2, 3]}"),

    "ltArrayDoesntMatchWhenFirstEltIsLarger" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [8, 2, 3]}"),

    "ltArrayMatchesWhenArrayIsShorter" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2]}"),

    "ltArrayDoesntMatchWhenArrayIsLonger" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2, 3, 4]}"),

    "gtArrayMatchesWholeArrayWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GTMatchExpr (parseValueOrDie "[1, 2]")))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "gtArrayDoesNotMatchWholeArrayWithImplicitArrayTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [1, 2]}"),

    "gtArrayMatchesWithImplicitTraversalDueToInnerArray" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [0, [1, 2, 3, 4]]}"),

    "gtArrayDoesNotMatchWithoutImplicitTraversalDespiteMatchingInnerArray" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (GTMatchExpr (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie "{\"a\": [0, [1, 2, 3, 4]]}")
    ]

typeBracketingTest :: Test
typeBracketingTest = TestList [
    "intIsNotLessThanString" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (LTMatchExpr (parseValueOrDie "\"foo\"")))
            (parseValueOrDie "1"),

    "intIsNotLessThanOrEqaulToString" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (LTEMatchExpr (parseValueOrDie "\"foo\"")))
            (parseValueOrDie "1"),

    "stringIsNotGreaterThanInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (GTMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "\"foo\""),

    "stringIsNotGreaterThanOrEqualToInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (GTEMatchExpr (parseValueOrDie "1")))
            (parseValueOrDie "\"foo\"")
    ]

elemMatchTests :: Test
elemMatchTests = TestList [
    "elemMatchObjectBasicMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": [{\"b\": 2}, {\"b\": 3}]}"),

    "elemMatchObjectBasicDoesNotMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": [{\"b\": 2}, {\"b\": 5}]}"),

    "elemMatchObjectDoesNotMatchNonArray" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": {\"b\": 3}}"),

    "elemMatchObjectMatchesWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": [{\"b\": 2}, {\"b\": 3}]}"),

    "elemMatchObjectDoesNotMatchWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": [{\"b\": 2}, {\"b\": 5}]}"),

    "elemMatchObjectDoesNotMatchNestedArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchExpr (parseValueOrDie "3")))))
            (parseValueOrDie "{\"a\": [[{\"b\": 2}, {\"b\": 3}]]}"),

    "nestedElemMatchObjMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b") (ElemMatchObjectExpr
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchExpr (parseValueOrDie "3")))))))
            (parseValueOrDie "{\"a\": [{\"b\": [{\"c\": [1, 2, 3]}]}]}"),

    "nestedElemMatchObjDoesNotMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b") (ElemMatchObjectExpr
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchExpr (parseValueOrDie "99")))))))
            (parseValueOrDie "{\"a\": [{\"b\": [{\"c\": [1, 2, 3]}]}]}"),

    "nestedElemMatchObjMatchesWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectExpr
                (PathAcceptingExpr (pathFromStringForTest "b*") (ElemMatchObjectExpr
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchExpr (parseValueOrDie "3")))))))
            (parseValueOrDie "{\"a\": [{\"b\": [{\"c\": [1, 2, 3]}]}]}"),

    "elemMatchValueBasicMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "3")]))
            (parseValueOrDie "{\"a\": [2, 3]}"),

    "elemMatchValueBasicDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "99")]))
            (parseValueOrDie "{\"a\": [2, 3]}"),

    "elemMatchValueMultiplePredsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValueExpr [
                GTMatchExpr (parseValueOrDie "5"),
                LTMatchExpr (parseValueOrDie "10")]))
            (parseValueOrDie "{\"a\": [2, 7, 12]}"),

    "elemMatchValueMultiplePredsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValueExpr [
                GTMatchExpr (parseValueOrDie "5"),
                LTMatchExpr (parseValueOrDie "10")]))
            (parseValueOrDie "{\"a\": [2, -1, 12]}"),

    "elemMatchValueWithNoPathMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (ElemMatchValueExpr [
                GTMatchExpr (parseValueOrDie "5"),
                LTMatchExpr (parseValueOrDie "10")]))
            (parseValueOrDie "[2, 7, 12]"),

    "elemMatchValueWithNoPathDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (ElemMatchValueExpr [
                GTMatchExpr (parseValueOrDie "5"),
                LTMatchExpr (parseValueOrDie "10")]))
            (parseValueOrDie "[2, -1, 12]"),

    "elemMatchValueNestedMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValueExpr [
                ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "2")]]))
            (parseValueOrDie "{\"a\": [0, [1, 2, 3]]}"),

    "elemMatchValueNestedDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValueExpr [
                ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "2")]]))
            (parseValueOrDie "{\"a\": [0, [1, 3]]}"),

    "elemMatchValueWithImplicitArrayTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "2")]))
            (parseValueOrDie "{\"a\": [1, 2, 3]}"),

    "elemMatchValueWithImplicitArrayTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (ElemMatchValueExpr [EqMatchExpr (parseValueOrDie "2")]))
            (parseValueOrDie "{\"a\": [[1, 2, 3]]}")
    ]

matchExprTest :: Test
matchExprTest = TestList [
    basicTests,
    comparisonToArrayTests,
    elemMatchTests,
    existsTests,
    inequalityTests,
    logicalExprTests,
    typeBracketingTest
    ]
