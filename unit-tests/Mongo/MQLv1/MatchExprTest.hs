{-# LANGUAGE QuasiQuotes #-}

module Mongo.MQLv1.MatchExprTest(
    matchExprTest
    ) where

import Mongo.Error
import Mongo.MQLv1.MatchExpr
import Mongo.MQLv1.Path
import Mongo.Value
import Test.HUnit
import Text.RawString.QQ

-- Tests for basic aspects of MatchExpr path traversal semantics, using an equality match
-- expression.
basicTests :: Test
basicTests = TestList [
    "equalityMatchEmptyPath" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "3"))) (parseValueOrDie "3"),

    "equalityMatchDoesNotMatchEmptyPath" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "3"))) (parseValueOrDie "6"),

    "equalityMatchDescendObjectPath" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"a": {"b": 3}}|]),

    "equalityMatchDescendObjectPathRepeatedFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.a") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"a": {"a": 3}}|]),

    "equalityMatchDoesNotMatchWhenPathMissing" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"a": {}}|]),

    "equalityConsidersFieldOrder" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (EqMatchPred (parseValueOrDie [r|{"a": 1, "b": 1}|])))
            (parseValueOrDie [r|{"b": 1, "a": 1}|]),

    "traversePathWithArrayIndices" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[1].$[2]")
                (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie "[0, [1, 2, 3], 4]"),

    "traversePathWithArrayIndicesRepeatedIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0].$[0]")
                (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie "[[3]]"),

    "traversePathFieldNameAndArrayIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "b.$[1]") (EqMatchPred (parseValueOrDie "2")))
            (parseValueOrDie [r|{"a": 0, "b": [1, 2, 3], "c": 4}|]),

    "traversePathFieldNameAndArrayIndexNoMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "b.$[1]")
                (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie [r|{"a": 0, "b": [1, 2, 3], "c": 4}|]),

    "equalityMatchWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "2")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "equalityMatchWithImplicitArrayTraversalNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "arrayDoesNotMatchScalarWithoutImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (EqMatchPred (parseValueOrDie "2")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "implicitArrayTraversalMultipleLevels" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") (EqMatchPred (parseValueOrDie "4")))
            (parseValueOrDie [r|{"a": [{"b": [1, 2]}, {"b": [3, 4]}]}|]),

    "implicitArrayTraversalMultipleLevelsNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie [r|{"a": [{"b": [1, 2]}, {"b": [3, 4]}]}|]),

    "implicitArrayTraversalWithArrayIndexPathComponent" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*") (EqMatchPred (parseValueOrDie "1")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNoMatchingValue" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*") (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "arrayDoesNotMatchScalarWithoutImplicitTraversalArrayPath" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]") (EqMatchPred (parseValueOrDie "1")))
            (parseValueOrDie "[[0, 1, 2], 3, 4]"),

    "implicitArrayTraversalWithArrayIndexPathComponentNested" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$[0]*.$[1]*")
                (EqMatchPred (parseValueOrDie "2")))
            (parseValueOrDie "[[[0, []], [1, [2]]]]"),

    "fieldNameOrIndexCanActAsIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchPred (parseValueOrDie "1")))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsIndexValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie "[0, 1, 2]"),

    "fieldNameOrIndexCanActAsFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"1": 3}|]),

    "repeatedFieldNameOrIndex" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<0>.$<0>")
                (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"0": {"0": 3}}|]),

    "fieldNameOrIndexCanActAsFieldNameValueDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>") (EqMatchPred (parseValueOrDie "99")))
            (parseValueOrDie [r|{"1": 3}|]),

    "fieldNameOrIndexActsAsFieldNameWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie [r|{"1": [1, 2, 3]}|]),

    -- XXX: This is an odd special case in MQLv1.
    "fieldNameOrIndexDoesNotUseImplicitTraversalWhenActsAsIndex" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*") (EqMatchPred (parseValueOrDie "3")))
            (parseValueOrDie "[0, [1, 2, 3]]")
    ]

-- Tests for $lt, $lte, $gt, $gte, and $ne.
inequalityTests :: Test
inequalityTests = TestList [
    "ltMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (LtMatchPred (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "ltDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (LtMatchPred (parseValueOrDie "0"))) (parseValueOrDie "1"),

    "ltMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (LtMatchPred (parseValueOrDie "0")))
        (parseValueOrDie [r|{"foo": [1, 2, -1]}|]),

    "lteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (LteMatchPred (parseValueOrDie "0"))) (parseValueOrDie "0"),

    "lteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (LteMatchPred (parseValueOrDie "0"))) (parseValueOrDie "2"),

    "lteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "foo*") (LteMatchPred (parseValueOrDie "0")))
            (parseValueOrDie [r|{"foo": [1, 2, -1]}|]),

    "gtMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (GtMatchPred (parseValueOrDie "0"))) (parseValueOrDie "1"),

    "gtDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (GtMatchPred (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "gtMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (GtMatchPred (parseValueOrDie "0")))
        (parseValueOrDie [r|{"foo": [0, 2, -1]}|]),

    "gteMatches" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr [] (GteMatchPred (parseValueOrDie "0"))) (parseValueOrDie "0"),

    "gteDoesntMatch" ~: "" ~: Right False ~=? evalMatchExpr
        (PathAcceptingExpr [] (GteMatchPred (parseValueOrDie "0"))) (parseValueOrDie "-1"),

    "gteMatchesWithImplicitTraversal" ~: "" ~: Right True ~=? evalMatchExpr
        (PathAcceptingExpr (pathFromStringForTest "foo*") (GteMatchPred (parseValueOrDie "0")))
        (parseValueOrDie [r|{"foo": [-3, 2, -1]}|]),

    "neMatchesUnequalInt" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (NotMatchPred (EqMatchPred (parseValueOrDie "1"))))
            (parseValueOrDie "2"),

    "neDoesntMatchEqualInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (NotMatchPred (EqMatchPred (parseValueOrDie "2"))))
            (parseValueOrDie "2"),

    "neMatchesArrayWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (NotMatchPred (EqMatchPred (parseValueOrDie "5"))))
            (parseValueOrDie [r|{"a": [3, 4]}|]),

    "neDoesntMatchArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (NotMatchPred (EqMatchPred (parseValueOrDie "4"))))
            (parseValueOrDie [r|{"a": [3, 4]}|]),

    "arrayNEArrayWithImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*")
                    (NotMatchPred (EqMatchPred (parseValueOrDie "[3, 4]"))))
            (parseValueOrDie [r|{"a": [3, 4]}|]),

    "arrayNEArrayWithImplicitTraversalDoesntMatchNested" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*")
                    (NotMatchPred (EqMatchPred (parseValueOrDie "[3, 4]"))))
            (parseValueOrDie [r|{"a": [0, [3, 4]]}|]),

    "arrayNEArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr
                (pathFromStringForTest "a*")
                    (NotMatchPred (EqMatchPred (parseValueOrDie "[3, 4]"))))
            (parseValueOrDie [r|{"a": [3, 5]}|]),

    "arrayNEArrayWithoutImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (NotMatchPred (EqMatchPred (parseValueOrDie "[3, 4]"))))
            (parseValueOrDie [r|{"a": [0, [3, 4]]}|])
    ]

-- Tests for $exists:true and $exists:false.
existsTests :: Test
existsTests = TestList [
    "existsMatchesIntWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchPred) (parseValueOrDie "3"),

    "existsMatchesNullWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchPred) (parseValueOrDie "null"),

    "existsMatchesUndefinedWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchPred) UndefinedValue,

    "existsMatchesEmptyArrayWithNoPath" ~: "" ~: Right True ~=?
        evalMatchExpr (PathAcceptingExpr [] ExistsMatchPred) (parseValueOrDie "[]"),

    "existsDoesNotMatchWhenPathDoesntExist" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchPred)
            (parseValueOrDie [r|{"a": 1}|]),

    "existsDoesNotMatchWhenPathDoesntExistWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchPred)
            (parseValueOrDie [r|{"a": [1, {"c": 2}]}|]),

    "existsMatchesWhenPathExistsMatchPredWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchPred)
            (parseValueOrDie [r|{"a": [1, {"b": 2}]}|]),

    "existsMatchesWhenPathExistsMatchPredWithImplicitTraversalTrailingEmptyArray" ~: "" ~:
        Right True ~=?
            evalMatchExpr
                (PathAcceptingExpr (pathFromStringForTest "a*.b*") ExistsMatchPred)
                (parseValueOrDie [r|{"a": [1, {"b": []}]}|]),

    "existsMatchesWhenPathExistsMatchPredWithImplicitTraversalArrIndexPath" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*.$<1>*") ExistsMatchPred)
            (parseValueOrDie "[0, [1, []], 2]"),

    "existsMatchesArrIndexResolvesToFieldName" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<1>*.$<1>*") ExistsMatchPred)
            (parseValueOrDie [r|{"1": {"1": []}}|]),

    "existsDoesNotMatchWhenArrIndexToHigh" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "$<8>*") ExistsMatchPred)
            (parseValueOrDie "[1, 2, 3]"),

    "notExistsMatchPredDoesNotMatchInt" ~: "" ~: Right False ~=?
        evalMatchExpr (NotMatchExpr (PathAcceptingExpr [] ExistsMatchPred)) (parseValueOrDie "3"),

    "notExistsMatchPredDoesNotMatchNull" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr [] ExistsMatchPred)) (parseValueOrDie "null"),

    "notExistsMatchPredDoesNotMatchUndefined" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr [] ExistsMatchPred)) (parseValueOrDie "undefined"),

    "notExistsMatchPredDoesNotMatchWhenObjectPathExistsMatchPred" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchPred))
            (parseValueOrDie [r|{"a": {"b": 1}}|]),

    "notExistsMatchPredMatchesWhenObjectPathDoesNotExist" ~: "" ~: Right True ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a.b") ExistsMatchPred))
            (parseValueOrDie [r|{"a": {"c": 1}}|]),

    "notExistsMatchPredDoesNotMatchEmptyArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (PathAcceptingExpr (pathFromStringForTest "a*") ExistsMatchPred))
            (parseValueOrDie [r|{"a": []}|])
    ]

-- Tests for $and, $or, $not, and $nor.
logicalExprTests :: Test
logicalExprTests = TestList [
    "emptyAndMatchesInt" ~: "" ~: Right True ~=?
        evalMatchExpr (AndMatchExpr []) (parseValueOrDie "1"),

    "andOfOneThingIsTheThingMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (AndMatchExpr [PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "1"))])
            (parseValueOrDie "1"),

    "andOfOneThingIsTheThingDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (AndMatchExpr [PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "99"))])
            (parseValueOrDie "1"),

    "andOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (AndMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "2")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "3"))])
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|]),

    "andOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (AndMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "2")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "99")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "3"))])
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|]),

    "emptyOrDoesntMatchInt" ~: "" ~: Right False ~=?
        evalMatchExpr (OrMatchExpr []) (parseValueOrDie "1"),

    "orOfOneThingIsTheThingMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (OrMatchExpr [PathAcceptingExpr[] (EqMatchPred (parseValueOrDie "1"))])
            (parseValueOrDie "1"),

    "orOfOneThingIsTheThingDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (OrMatchExpr [PathAcceptingExpr[] (EqMatchPred (parseValueOrDie "99"))])
            (parseValueOrDie "1"),

    "orOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "9"))])
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|]),

    "orOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "7")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "9"))])
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|]),

    "emptyNorMatchesInt" ~: "" ~: Right True ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [])) (parseValueOrDie "1"),

    "norOfOneThingIsNotMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (NotMatchExpr (OrMatchExpr [PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "99"))]))
            (parseValueOrDie "1"),

    "norOfOneThingIsNotDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (NotMatchExpr (OrMatchExpr [PathAcceptingExpr [] (EqMatchPred (parseValueOrDie "1"))]))
            (parseValueOrDie "1"),

    "norOfSeveralThingsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "7")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "9"))]))
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|]),

    "norOfSeveralThingsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr (NotMatchExpr (OrMatchExpr [
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "8")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "1")),
            PathAcceptingExpr (pathFromStringForTest "a*") (EqMatchPred (parseValueOrDie "9"))]))
                (parseValueOrDie [r|{"a": [0, 1, 2, 3]}|])
    ]

comparisonToArrayTests :: Test
comparisonToArrayTests = TestList [
    "eqArrayWithoutImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (EqMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "eqArrayWithoutImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (EqMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2, 99]}|]),

    "eqWholeArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "eqNestedArrayWithImplicitTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [0, [1, 2, 3]]}|]),

    "eqNestedArrayWithImplicitTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (EqMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [0, [1, 2, 3, 4]]}|]),

    "ltArrayMatchesWhenFirstEltIsSmaller" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [0, 2, 3]}|]),

    "ltArrayDoesntMatchWhenFirstEltIsLarger" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [8, 2, 3]}|]),

    "ltArrayMatchesWhenArrayIsShorter" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2]}|]),

    "ltArrayDoesntMatchWhenArrayIsLonger" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (LtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2, 3, 4]}|]),

    "gtArrayMatchesWholeArrayWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GtMatchPred (parseValueOrDie "[1, 2]")))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "gtArrayDoesNotMatchWholeArrayWithImplicitArrayTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [1, 2]}|]),

    "gtArrayMatchesWithImplicitTraversalDueToInnerArray" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (GtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [0, [1, 2, 3, 4]]}|]),

    "gtArrayDoesNotMatchWithoutImplicitTraversalDespiteMatchingInnerArray" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (GtMatchPred (parseValueOrDie "[1, 2, 3]")))
            (parseValueOrDie [r|{"a": [0, [1, 2, 3, 4]]}|])
    ]

typeBracketingTest :: Test
typeBracketingTest = TestList [
    "intIsNotLessThanString" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (LtMatchPred (parseValueOrDie [r|"foo"|])))
            (parseValueOrDie "1"),

    "intIsNotLessThanOrEqaulToString" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (LteMatchPred (parseValueOrDie [r|"foo"|])))
            (parseValueOrDie "1"),

    "stringIsNotGreaterThanInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (GtMatchPred (parseValueOrDie "1")))
            (parseValueOrDie [r|"foo"|]),

    "stringIsNotGreaterThanOrEqualToInt" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (GteMatchPred (parseValueOrDie "1")))
            (parseValueOrDie [r|"foo"|])
    ]

elemMatchTests :: Test
elemMatchTests = TestList [
    "elemMatchObjectBasicMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": [{"b": 2}, {"b": 3}]}|]),

    "elemMatchObjectBasicDoesNotMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": [{"b": 2}, {"b": 5}]}|]),

    "elemMatchObjectDoesNotMatchNonArray" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": {"b": 3}}|]),

    "elemMatchObjectMatchesWithImplicitArrayTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": [{"b": 2}, {"b": 3}]}|]),

    "elemMatchObjectDoesNotMatchWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": [{"b": 2}, {"b": 5}]}|]),

    "elemMatchObjectDoesNotMatchNestedArrayWithImplicitTraversal" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b*")
                    (EqMatchPred (parseValueOrDie "3")))))
            (parseValueOrDie [r|{"a": [[{"b": 2}, {"b": 3}]]}|]),

    "nestedElemMatchObjMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b") (ElemMatchObjectPred
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchPred (parseValueOrDie "3")))))))
            (parseValueOrDie [r|{"a": [{"b": [{"c": [1, 2, 3]}]}]}|]),

    "nestedElemMatchObjDoesNotMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b") (ElemMatchObjectPred
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchPred (parseValueOrDie "99")))))))
            (parseValueOrDie [r|{"a": [{"b": [{"c": [1, 2, 3]}]}]}|]),

    "nestedElemMatchObjMatchesWithImplicitTraversal" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*") (ElemMatchObjectPred
                (PathAcceptingExpr (pathFromStringForTest "b*") (ElemMatchObjectPred
                    (PathAcceptingExpr (pathFromStringForTest "c*")
                        (EqMatchPred (parseValueOrDie "3")))))))
            (parseValueOrDie [r|{"a": [{"b": [{"c": [1, 2, 3]}]}]}|]),

    "elemMatchValueBasicMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "3")])))
            (parseValueOrDie [r|{"a": [2, 3]}|]),

    "elemMatchValueBasicDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "99")])))
            (parseValueOrDie [r|{"a": [2, 3]}|]),

    "elemMatchValueMultiplePredsMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValuePred (AndMatchPred [
                GtMatchPred (parseValueOrDie "5"),
                LtMatchPred (parseValueOrDie "10")])))
            (parseValueOrDie [r|{"a": [2, 7, 12]}|]),

    "elemMatchValueMultiplePredsDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a") (ElemMatchValuePred (AndMatchPred [
                GtMatchPred (parseValueOrDie "5"),
                LtMatchPred (parseValueOrDie "10")])))
            (parseValueOrDie [r|{"a": [2, -1, 12]}|]),

    "elemMatchValueWithNoPathMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (ElemMatchValuePred (AndMatchPred [
                GtMatchPred (parseValueOrDie "5"),
                LtMatchPred (parseValueOrDie "10")])))
            (parseValueOrDie "[2, 7, 12]"),

    "elemMatchValueWithNoPathDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr [] (ElemMatchValuePred (AndMatchPred [
                GtMatchPred (parseValueOrDie "5"),
                LtMatchPred (parseValueOrDie "10")])))
            (parseValueOrDie "[2, -1, 12]"),

    "elemMatchValueNestedMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValuePred (AndMatchPred [
                    ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "2")])])))
            (parseValueOrDie [r|{"a": [0, [1, 2, 3]]}|]),

    "elemMatchValueNestedDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a")
                (ElemMatchValuePred (AndMatchPred [
                    ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "2")])])))
            (parseValueOrDie [r|{"a": [0, [1, 3]]}|]),

    "elemMatchValueWithImplicitArrayTraversalMatches" ~: "" ~: Right True ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "2")])))
            (parseValueOrDie [r|{"a": [1, 2, 3]}|]),

    "elemMatchValueWithImplicitArrayTraversalDoesntMatch" ~: "" ~: Right False ~=?
        evalMatchExpr
            (PathAcceptingExpr (pathFromStringForTest "a*")
                (ElemMatchValuePred (AndMatchPred [EqMatchPred (parseValueOrDie "2")])))
            (parseValueOrDie [r|{"a": [[1, 2, 3]]}|])
    ]

-- Tests for the MatchExpr parser.
parserTests :: Test
parserTests = TestList [
    "invalidJSONFailsToParse" ~: "" ~: InvalidJSON ~=? getErrCode (parseMatchExprString "{"),

    "scalarMatchExprFailsToParse" ~: "" ~: FailedToParse ~=? getErrCode (parseMatchExprString "1"),

    "unknownTopLevelOperatorFailsToParse" ~: "" ~: FailedToParse ~=?
        getErrCode (parseMatchExprString [r|{"$unknown": 1}|]),

    "unknownPathAcceptingMatchExprFailsToParse" ~: "" ~: FailedToParse ~=?
        getErrCode (parseMatchExprString [r|{"path": {"$unknown": 1}}|]),

    "explicitEqParses" ~: "" ~:
        Right (PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
            (EqMatchPred (IntValue 3))) ~=?
        parseMatchExprString [r|{"a": {"$eq": 3}}|],

    "implicitEqParses" ~: "" ~:
        Right (PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
            (EqMatchPred (IntValue 3))) ~=?
        parseMatchExprString [r|{"a": 3}|],

    "twoImplicitEqWithImplicitAnd" ~: "" ~:
        Right (AndMatchExpr [
            PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3)),
            PathAcceptingExpr [PathComponent (FieldName "b") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 4))]) ~=?
        parseMatchExprString [r|{"a": 3, "b": 4}|],

    "parseExplicitSingletonAnd" ~: "" ~:
        Right (AndMatchExpr [
            PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3))])  ~=?
        parseMatchExprString [r|{"$and": [{"a": {"$eq": 3}}]}|],

    "zeroCanActAsArrayIndex" ~: "" ~:
        Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldNameOrArrayIndex 0) ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3)))  ~=?
        parseMatchExprString [r|{"a.0":  3}|],

    "tenCanActAsArrayIndex" ~: "" ~:
        Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldNameOrArrayIndex 10) ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3)))  ~=?
        parseMatchExprString [r|{"a.10":  3}|],

    "zeroZeroCannotActAsArrayIndex" ~: "" ~:
        Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldName "00") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3)))  ~=?
        parseMatchExprString [r|{"a.00":  3}|],

    "zeroOneCannotActAsArrayIndex" ~: "" ~:
        Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldName "01") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 3)))  ~=?
        parseMatchExprString [r|{"a.01":  3}|],

    "notWithScalarFailsToParse" ~: "" ~: FailedToParse ~=?
        getErrCode (parseMatchExprString [r|{"a": {"$not": 1}}|]),

    "notWithEmptyObjectFailsToParse" ~: "" ~: FailedToParse ~=?
        getErrCode (parseMatchExprString [r|{"a": {"$not": {}}}|]),

    "notWithTopLevelExprOperatorFailsToParse" ~: "" ~: FailedToParse ~=?
        getErrCode (parseMatchExprString [r|{"a": {"$not": {"$and": [{"b": 1}]}}}|]),

    "notWithLtParses" ~: "" ~:
        Right (NotMatchExpr (PathAcceptingExpr
            [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
            (AndMatchPred [LtMatchPred (IntValue 0)]))) ~=?
        parseMatchExprString [r|{"a": {"$not": {"$lt": 0}}}|],

    "notWithLtAndGtParses" ~: "" ~:
        Right (NotMatchExpr (PathAcceptingExpr
            [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
            (AndMatchPred [LtMatchPred (IntValue 0), GtMatchPred (IntValue 0)]))) ~=?
        parseMatchExprString [r|{"a": {"$not": {"$lt": 0, "$gt": 0}}}|],

    "notAsSiblingOfNonNotParses" ~: "" ~:
        Right (AndMatchExpr [
            NotMatchExpr (PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
                (AndMatchPred [LtMatchPred (IntValue 0)])),
            PathAcceptingExpr [PathComponent (FieldName "a") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 8))]) ~=?
        parseMatchExprString [r|{"a": {"$not": {"$lt": 0}, "$eq": 8}}|],

    "canParseNe" ~: "" ~: Right (NotMatchExpr (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] (EqMatchPred (IntValue 8)))) ~=?
        parseMatchExprString [r|{"a": {"$ne": 8}}|],

    "canParseLt" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] (LtMatchPred (IntValue 8))) ~=?
        parseMatchExprString [r|{"a": {"$lt": 8}}|],

    "canParseLte" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] (LteMatchPred (IntValue 8))) ~=?
        parseMatchExprString [r|{"a": {"$lte": 8}}|],

    "canParseGt" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] (GtMatchPred (IntValue 8))) ~=?
        parseMatchExprString [r|{"a": {"$gt": 8}}|],

    "canParseGte" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] (GteMatchPred (IntValue 8))) ~=?
        parseMatchExprString [r|{"a": {"$gte": 8}}|],

    "canParseExistsTrue" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] ExistsMatchPred) ~=?
        parseMatchExprString [r|{"a": {"$exists": true}}|],

    "canParseExistsWithTruthyValue" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] ExistsMatchPred) ~=?
        parseMatchExprString [r|{"a": {"$exists": {}}}|],

    "canParseExistsFalse" ~: "" ~: Right (NotMatchExpr (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] ExistsMatchPred)) ~=?
        parseMatchExprString [r|{"a": {"$exists": false}}|],

    "canParseExistsFalsyValue" ~: "" ~: Right (NotMatchExpr (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays] ExistsMatchPred)) ~=?
        parseMatchExprString [r|{"a": {"$exists": 0}}|],

    "canParseElemMatchObject" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays]
                (ElemMatchObjectPred (PathAcceptingExpr [
                    PathComponent (FieldName "b") ImplicitlyTraverseArrays,
                    PathComponent (FieldName "c") ImplicitlyTraverseArrays]
                        (EqMatchPred (IntValue 0))))) ~=?
        parseMatchExprString [r|{"a": {"$elemMatch": {"b.c": 0}}}|],

    "canParseElemMatchValue" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldName "b") ImplicitlyTraverseArrays]
                (ElemMatchValuePred (AndMatchPred [
                    GtMatchPred (IntValue 0),
                    LtMatchPred (IntValue 9)]))) ~=?
        parseMatchExprString [r|{"a.b": {"$elemMatch": {"$gt": 0, "$lt": 9}}}|],

    "canParseElemMatchValueWithNot" ~: "" ~: Right (PathAcceptingExpr [
            PathComponent (FieldName "a") ImplicitlyTraverseArrays,
            PathComponent (FieldName "b") ImplicitlyTraverseArrays]
                (ElemMatchValuePred (AndMatchPred
                    [NotMatchPred (AndMatchPred [LtMatchPred (IntValue 0)])]))) ~=?
        parseMatchExprString [r|{"a.b": {"$elemMatch": {"$not": {"$lt": 0}}}}|],

    "canParseElemMatchValueInsideElemMatchObject" ~: "" ~: Right (PathAcceptingExpr [
        PathComponent (FieldName "a") ImplicitlyTraverseArrays]
        (ElemMatchObjectPred (AndMatchExpr [
            PathAcceptingExpr [PathComponent (FieldName "b") ImplicitlyTraverseArrays]
                (EqMatchPred (IntValue 1)),
            PathAcceptingExpr [PathComponent (FieldName "c") ImplicitlyTraverseArrays]
                (ElemMatchValuePred (AndMatchPred [
                GteMatchPred (IntValue 2),
                LteMatchPred (IntValue 3)]))]))) ~=?
        parseMatchExprString [r|{"a": {"$elemMatch":
            {"b": 1, "c": {"$elemMatch": {"$gte": 2, "$lte": 3}}}}}|]
    ]

-- Tests that we can perform match expression evaluation correctly where the match expression and
-- the document to match are both represented as extended JSON strings.
endToEndTests :: Test
endToEndTests = TestList [
    "eqMatches" ~: "" ~: Right True ~=?
        evalStringMatchExpr [r|{"a": 3}|] [r|{"a": 3}|],

    "eqDoesntMatch" ~: "" ~: Right False ~=?
        evalStringMatchExpr [r|{"a": 3}|] [r|{"a": 99}|],

    "elemMatchValueDoesntLiftNotMatches" ~: "" ~: Right True ~=?
        evalStringMatchExpr
            [r|{"a": {"$elemMatch": {"$not": {"$gt": 0}}}}|]
            [r|{"a": [1, -3, 2]}|],

    "elemMatchValueDoesntLiftNotNoMatch" ~: "" ~: Right False ~=?
        evalStringMatchExpr
            [r|{"a": {"$elemMatch": {"$not": {"$gt": 0}}}}|]
            [r|{"a": [1, 3, 2]}|],

    "elemMatchObjectWithExistsMatches" ~: "" ~: Right True ~=?
        evalStringMatchExpr
            [r|{"a": {"$elemMatch": {"b": {"$exists": true}, "c": 1}}}|]
            [r|{"a": [{"b": 1}, {"b": 1, "c": 1}, {"c": 1}]}|],

    "elemMatchObjectWithExistsDoesntMatch" ~: "" ~: Right False ~=?
        evalStringMatchExpr
            [r|{"a": {"$elemMatch": {"b": {"$exists": true}, "c": 1}}}|]
            [r|{"a": [{"b": 1}, {"c": 1}]}|]
    ]

matchExprTest :: Test
matchExprTest = TestList [
    basicTests,
    comparisonToArrayTests,
    elemMatchTests,
    endToEndTests,
    existsTests,
    inequalityTests,
    logicalExprTests,
    parserTests,
    typeBracketingTest
    ]
