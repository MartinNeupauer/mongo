module Mongo.MQLv1.PathTest(
    pathTest
    ) where

import Mongo.MQLv1.Path
import Test.HUnit

pathTest :: Test
pathTest = TestList [
    "singleComponentFieldName" ~: "" ~:
        [PathComponent (FieldName "foo") NoImplicitTraversal] ~=?
        pathFromStringForTest "foo",

    "singleComponentFieldNameImplicitTraversal" ~: "" ~:
        [PathComponent (FieldName "foo") ImplicitlyTraverseArrays] ~=?
        pathFromStringForTest "foo*",

    "singleComponentArrayIndex" ~: "" ~:
        [PathComponent (ArrayIndex 3) NoImplicitTraversal] ~=?
        pathFromStringForTest "$[3]",

    "singleComponentArrayIndexImplicitTraversal" ~: "" ~:
        [PathComponent (ArrayIndex 3) ImplicitlyTraverseArrays] ~=?
        pathFromStringForTest "$[3]*",

    "singleComponentEither" ~: "" ~:
        [PathComponent (FieldNameOrArrayIndex 0) NoImplicitTraversal] ~=?
        pathFromStringForTest "$<0>",

    "singleComponentEitherImplicitTraversal" ~: "" ~:
        [PathComponent (FieldNameOrArrayIndex 0) ImplicitlyTraverseArrays] ~=?
        pathFromStringForTest "$<0>*",

    "multipleComponentsBasic" ~: "" ~:
        [PathComponent (FieldName "a") NoImplicitTraversal,
         PathComponent (FieldName "b") NoImplicitTraversal,
         PathComponent (FieldName "c") NoImplicitTraversal] ~=?
        pathFromStringForTest "a.b.c",

    "multipleComponentsMixed" ~: "" ~:
        [PathComponent (FieldName "a") NoImplicitTraversal,
         PathComponent (FieldName "b") ImplicitlyTraverseArrays,
         PathComponent (ArrayIndex 0) NoImplicitTraversal,
         PathComponent (ArrayIndex 1) ImplicitlyTraverseArrays,
         PathComponent (FieldNameOrArrayIndex 2) NoImplicitTraversal,
         PathComponent (FieldNameOrArrayIndex 3) ImplicitlyTraverseArrays] ~=?
        pathFromStringForTest "a.b*.$[0].$[1]*.$<2>.$<3>*"
    ]
