module Mongo.ValueTest(
    valueTest
    ) where

import Mongo.Value
import Test.HUnit

valueTest :: Test
valueTest = TestList [
    "parseNullScalar" ~: "" ~: Right NullValue ~=? valueFromString "null",

    "parseIntValue" ~: "" ~: Right (IntValue 345) ~=? valueFromString "345",

    "parseTrueValue" ~: "" ~: Right (BoolValue True) ~=? valueFromString "true",

    "parseFalseValue" ~: "" ~: Right (BoolValue False) ~=? valueFromString "false",

    "parseStringValue" ~: "" ~: Right (StringValue "foo") ~=? valueFromString "\"foo\"",

    "parseArrayValue" ~: "" ~:
        Right (ArrayValue Array { getElements = [IntValue 1, IntValue 2] }) ~=?
            valueFromString "[1, 2]",

    "parseDocumentValue" ~: "" ~: Right (DocumentValue Document { getFields = [
        ("a", NullValue),
        ("b", ArrayValue Array { getElements = [] }),
        ("c", DocumentValue Document { getFields = [] }),
        ("d", BoolValue True)
        ] }) ~=?
            valueFromString "{\"a\": null, \"b\": [], \"c\": {}, \"d\": true}",

    "nullEqualsNull" ~: "" ~: True ~=? parseValueOrDie "null" `compareEQ` parseValueOrDie "null",
    "nullLTENull" ~: "" ~: True ~=? parseValueOrDie "null" `compareLTE` parseValueOrDie "null",
    "nullGTENull" ~: "" ~: True ~=? parseValueOrDie "null" `compareGTE` parseValueOrDie "null",
    "nullNotLTNull" ~: "" ~: False ~=? parseValueOrDie "null" `compareLT` parseValueOrDie "null",
    "nullNotGTNull" ~: "" ~: False ~=? parseValueOrDie "null" `compareGT` parseValueOrDie "null",

    "undefinedLTNull" ~: "" ~: LT ~=? UndefinedValue `compareValues` NullValue,
    "nullGTUndefiend" ~: "" ~: GT ~=? NullValue `compareValues` UndefinedValue,

    "int0LTInt5" ~: "" ~: LT ~=? parseValueOrDie "0" `compareValues` parseValueOrDie "5",

    "emptyObjLTNonEmpty" ~: "" ~: LT ~=?
        parseValueOrDie "{}" `compareValues` parseValueOrDie "{\"a\": 1}",

    "nonEmptyObjGTEmpty" ~: "" ~: GT ~=?
        parseValueOrDie "{\"a\": 1}" `compareValues` parseValueOrDie "{}",

    "equalObjects" ~: "" ~: EQ ~=?
        parseValueOrDie "{\"a\": 1, \"b\": 2}" `compareValues`
        parseValueOrDie "{\"a\": 1, \"b\": 2}",

    "objectLTDueToFieldName" ~: "" ~: LT ~=?
        parseValueOrDie "{\"a\": 1, \"b\": 2}" `compareValues`
        parseValueOrDie "{\"a\": 1, \"c\": 2}",

    "objectLTDueToValue" ~: "" ~: LT ~=?
        parseValueOrDie "{\"a\": 1, \"b\": 2}" `compareValues`
        parseValueOrDie "{\"a\": 1, \"c\": 3}",

    "emptyArrLTNonEmpty" ~: "" ~: LT ~=? parseValueOrDie "[]" `compareValues` parseValueOrDie "[1]",

    "nonEmptyArrGTEmpty" ~: "" ~: GT ~=? parseValueOrDie "[1]" `compareValues` parseValueOrDie "[]",

    "equalArrays" ~: "" ~: EQ ~=? parseValueOrDie "[1, 0, 3]" `compareValues`
        parseValueOrDie "[1, 0, 3]",

    "arrayLT" ~: "" ~: LT ~=? parseValueOrDie "[1, 0, 10]" `compareValues`
        parseValueOrDie "[1, 3, 3]",

    "stringIsGreaterThanInt" ~: "" ~: GT ~=?
        parseValueOrDie "\"foo\"" `compareValues` parseValueOrDie "1",

    "intIsLessThanString" ~: "" ~: LT ~=?
        parseValueOrDie "1" `compareValues` parseValueOrDie "\"foo\""
    ]
