{-# LANGUAGE QuasiQuotes #-}

module Mongo.MQLv1.ProjectionTest(
    projectionTest
    ) where

import Mongo.Error
import Mongo.MQLv1.Path
import Mongo.MQLv1.Projection
import Mongo.Value
import Test.HUnit
import Text.RawString.QQ

leaf :: InclusionProjectionNode
leaf = InclusionProjectionNode []

parseDocumentOrDie :: String -> Document
parseDocumentOrDie str = case valueFromString str of
    Right (DocumentValue doc) -> doc
    Right _ -> error $ "String parsed to something other than Document: " ++ str
    Left Error { errCode = code, errReason = reason } -> error reason

projectionTest :: Test
projectionTest = TestList [
    "includeFieldInSingleFieldDoc" ~: "" ~: Right (parseDocumentOrDie [r|{"a": 1}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie [r|{"a": 1}|]),

    "includeSingleFieldInMultiFieldDoc" ~: "" ~: Right (parseDocumentOrDie [r|{"a": 2}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie [r|{"x": 1, "a": 2, "y": {"z": 3}}|]),

    "includeMultipleFieldsTopLevel" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": 2, "y": {"z": 3}}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [
                (pathCompFromStringForTest "y", leaf),
                (pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie [r|{"x": 1, "a": 2, "y": {"z": 3}, "w": 4}|]),

    "includeFieldsFromSameSubdoc" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": {"b": 3, "c": 4}}|]) ~=?
        evalInclusionProjection
            -- {"a.b": 1, "a.c": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a",
                InclusionProjectionNode [
                    (pathCompFromStringForTest "b", leaf),
                    (pathCompFromStringForTest "c", leaf)])])
            (parseDocumentOrDie [r|{"x": 1, "a": {"y": 2, "b": 3, "c": 4}}|]),

    "inclusionPathTraversesThroughInt" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": {}}|]) ~=?
        evalInclusionProjection
            -- {"a.b.c": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a",
                InclusionProjectionNode [
                    (pathCompFromStringForTest "b",
                        InclusionProjectionNode [
                            (pathCompFromStringForTest "c", leaf)])])])
            (parseDocumentOrDie [r|{"a": {"b": 1}}|]),

    "includeThroughArrayWithNoImplicitTraversal" ~: "" ~:
        Right (parseDocumentOrDie "{}") ~=?
        evalInclusionProjection
            -- {"a.b": 1, "a.c": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a",
                InclusionProjectionNode [(pathCompFromStringForTest "b", leaf)])])
            (parseDocumentOrDie [r|{"a": [{"b": 1}, {"b": 2}]}|]),

    "includeEntireArrayWithNoImplicitTraversal" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": [{"b": 1}, {"b": 2}]}|]) ~=?
        evalInclusionProjection
            -- {"a": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie [r|{"a": [{"b": 1}, {"b": 2}], "c": 3}|]),

    "includeUnbalancedProjectionTree" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": 2, "b": {"c": 4, "d": {"e": 7}}}|]) ~=?
        evalInclusionProjection
            -- {a: 1, "b.c": 1, "b.d.e": 1}
            (InclusionProjectionNode [
                (pathCompFromStringForTest "a", leaf),
                (pathCompFromStringForTest "b",
                    InclusionProjectionNode [
                        (pathCompFromStringForTest "c", leaf),
                        (pathCompFromStringForTest "d",
                            InclusionProjectionNode [
                                (pathCompFromStringForTest "e", leaf)])])])
            (parseDocumentOrDie [r|{"x":1,"a":2,"b":{"y":3,"c":4,"z":5,"d":{"w":6,"e":7}}}|]),

    "includeFieldInsideArrayWithImplicitTraversal" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": [{"b": 2}, {"b": 4}]}|]) ~=?
        evalInclusionProjection
            -- {"a.b": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a*",
                InclusionProjectionNode [(pathCompFromStringForTest "b*", leaf)])])
            (parseDocumentOrDie [r|{"x": 1, "a": [{"b": 2, "c": 3}, {"b": 4, "c": 5}]}|]),

    "inclusionWithImplicitTraversalExcludesScalars" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": []}|]) ~=?
        evalInclusionProjection
            -- {"a.b": 1}.
            (InclusionProjectionNode [(pathCompFromStringForTest "a*",
                InclusionProjectionNode [(pathCompFromStringForTest "b*", leaf)])])
            (parseDocumentOrDie [r|{"a": [1, 2]}|]),

    "inclusionOfWholeArrayWithImplicitTraversal" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": [1, 2]}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a*", leaf)])
            (parseDocumentOrDie [r|{"a": [1, 2]}|]),

    "fieldNameOrArrayIndexCompResultsInIllegalProjectionError" ~: "" ~: IllegalProjection ~=?
        getErrCode (evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a",
                InclusionProjectionNode [(pathCompFromStringForTest "$<0>", leaf)])])
            (parseDocumentOrDie [r|{"a": [1, 2]}|])),

    "arrayIndexPathComponentNotYetImplemented" ~: "" ~: NotImplemented ~=?
        getErrCode (evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a",
                InclusionProjectionNode [(pathCompFromStringForTest "$[0]", leaf)])])
            (parseDocumentOrDie [r|{"a": [1, 2]}|])),

    "maintainFieldOrderingOfDoc" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": 2, "b": 4}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [
                (pathCompFromStringForTest "b", leaf),
                (pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie [r|{"x": 1, "a": 2, "y": 3, "b": 4}|]),

    "emptyProjectionResultsInEmptyDoc" ~: "" ~: Right (parseDocumentOrDie "{}") ~=?
        evalInclusionProjection
            (InclusionProjectionNode [])
            (parseDocumentOrDie [r|{"x": 1, "a": 2, "y": 3, "b": 4}|]),

    "projectingEmptyDocResultsInEmptyDoc" ~: "" ~: Right (parseDocumentOrDie "{}") ~=?
        evalInclusionProjection
            (InclusionProjectionNode [
                (pathCompFromStringForTest "b",
                    InclusionProjectionNode [(pathCompFromStringForTest "c", leaf)]),
                (pathCompFromStringForTest "a", leaf)])
            (parseDocumentOrDie "{}"),

    "projectionResultsInEmptySubDoc" ~: "" ~: Right (parseDocumentOrDie [r|{"a": {}}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a*",
                InclusionProjectionNode [(pathCompFromStringForTest "c*", leaf)])])
            (parseDocumentOrDie [r|{"a": {"b": 1}}|]),

    "projectionResultsInEmptySubDocDuringImplicitTraversal" ~: "" ~:
        Right (parseDocumentOrDie [r|{"a": [{}, {"c": 1}]}|]) ~=?
        evalInclusionProjection
            (InclusionProjectionNode [(pathCompFromStringForTest "a*",
                InclusionProjectionNode [(pathCompFromStringForTest "c*", leaf)])])
            (parseDocumentOrDie [r|{"a": [{"b": 1}, {"c": 1}]}|])
    ]
