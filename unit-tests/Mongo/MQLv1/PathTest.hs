module Mongo.MQLv1.PathTest(
    pathTest
    ) where

import Mongo.MQLv1.Path
import qualified Test.HUnit as HUnit

singleComponentFieldName = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldName "foo") NoImplicitTraversal]
    (pathFromStringForTest "foo"))

singleComponentFieldNameImplicitTraversal = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldName "foo") ImplicitlyTraverseArrays]
    (pathFromStringForTest "foo*"))

singleComponentArrayIndex = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (ArrayIndex 3) NoImplicitTraversal]
    (pathFromStringForTest "$[3]"))

singleComponentArrayIndexImplicitTraversal = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (ArrayIndex 3) ImplicitlyTraverseArrays]
    (pathFromStringForTest "$[3]*"))

singleComponentEither = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldNameOrArrayIndex 0) NoImplicitTraversal]
    (pathFromStringForTest "$<0>"))

singleComponentEitherImplicitTraversal = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldNameOrArrayIndex 0) ImplicitlyTraverseArrays]
    (pathFromStringForTest "$<0>*"))

multipleComponentsBasic = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldName "a") NoImplicitTraversal,
     PathComponent (FieldName "b") NoImplicitTraversal,
     PathComponent (FieldName "c") NoImplicitTraversal]
    (pathFromStringForTest "a.b.c"))

multipleComponentsMixed = HUnit.TestCase (HUnit.assertEqual ""
    [PathComponent (FieldName "a") NoImplicitTraversal,
     PathComponent (FieldName "b") ImplicitlyTraverseArrays,
     PathComponent (ArrayIndex 0) NoImplicitTraversal,
     PathComponent (ArrayIndex 1) ImplicitlyTraverseArrays,
     PathComponent (FieldNameOrArrayIndex 2) NoImplicitTraversal,
     PathComponent (FieldNameOrArrayIndex 3) ImplicitlyTraverseArrays]
    (pathFromStringForTest "a.b*.$[0].$[1]*.$<2>.$<3>*"))

pathTest :: HUnit.Test
pathTest = HUnit.TestList [
    HUnit.TestLabel "singleComponentFieldName" singleComponentFieldName,
    HUnit.TestLabel "singleComponentFieldNameImplicitTraversal"
        singleComponentFieldNameImplicitTraversal,
    HUnit.TestLabel "singleComponentArrayIndex" singleComponentArrayIndex,
    HUnit.TestLabel "singleComponentArrayIndexImplicitTraversal"
        singleComponentArrayIndexImplicitTraversal,
    HUnit.TestLabel "singleComponentEither" singleComponentEither,
    HUnit.TestLabel "singleComponentEitherImplicitTraversal" singleComponentEitherImplicitTraversal,
    HUnit.TestLabel "multipleComponentsBasic" multipleComponentsBasic,
    HUnit.TestLabel "multipleComponentsMixed" multipleComponentsMixed
    ]
