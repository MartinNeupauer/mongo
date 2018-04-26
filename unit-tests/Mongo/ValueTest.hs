{-# LANGUAGE QuasiQuotes #-}

module Mongo.ValueTest(
    valueTest
    ) where

import Data.Char (isSeparator, isSpace)
import Mongo.Error
import Mongo.Value
import Test.HUnit
import Text.RawString.QQ

basicTests :: Test
basicTests = TestList [
    "parseNullScalar" ~: "" ~: Right NullValue ~=? valueFromString "null",

    "parseIntValue" ~: "" ~: Right (IntValue 345) ~=? valueFromString "345",

    "parseTrueValue" ~: "" ~: Right (BoolValue True) ~=? valueFromString "true",

    "parseFalseValue" ~: "" ~: Right (BoolValue False) ~=? valueFromString "false",

    "parseStringValue" ~: "" ~: Right (StringValue "foo") ~=? valueFromString [r|"foo"|],

    "parseArrayValue" ~: "" ~:
        Right (ArrayValue Array { getElements = [IntValue 1, IntValue 2] }) ~=?
            valueFromString "[1, 2]",

    "parseDocumentValue" ~: "" ~: Right (DocumentValue Document { getFields = [
        ("a", NullValue),
        ("b", ArrayValue Array { getElements = [] }),
        ("c", DocumentValue Document { getFields = [] }),
        ("d", BoolValue True)
        ] }) ~=?
            valueFromString [r|{"a": null, "b": [], "c": {}, "d": true}|],

    "nullEqualsNull" ~: "" ~: True ~=? parseValueOrDie "null" `compareEQ` parseValueOrDie "null",
    "nullLTENull" ~: "" ~: True ~=? parseValueOrDie "null" `compareLTE` parseValueOrDie "null",
    "nullGTENull" ~: "" ~: True ~=? parseValueOrDie "null" `compareGTE` parseValueOrDie "null",
    "nullNotLTNull" ~: "" ~: False ~=? parseValueOrDie "null" `compareLT` parseValueOrDie "null",
    "nullNotGTNull" ~: "" ~: False ~=? parseValueOrDie "null" `compareGT` parseValueOrDie "null",

    "undefinedLTNull" ~: "" ~: LT ~=? UndefinedValue `compareValues` NullValue,
    "nullGTUndefiend" ~: "" ~: GT ~=? NullValue `compareValues` UndefinedValue,

    "int0LTInt5" ~: "" ~: LT ~=? parseValueOrDie "0" `compareValues` parseValueOrDie "5",

    "emptyObjLTNonEmpty" ~: "" ~: LT ~=?
        parseValueOrDie [r|{}|] `compareValues` parseValueOrDie [r|{"a": 1}|],

    "nonEmptyObjGTEmpty" ~: "" ~: GT ~=?
        parseValueOrDie [r|{"a": 1}|] `compareValues` parseValueOrDie [r|{}|],

    "equalObjects" ~: "" ~: EQ ~=?
        parseValueOrDie [r|{"a": 1, "b": 2}|] `compareValues`
        parseValueOrDie [r|{"a": 1, "b": 2}|],

    "objectLTDueToFieldName" ~: "" ~: LT ~=?
        parseValueOrDie [r|{"a": 1, "b": 2}|] `compareValues`
        parseValueOrDie [r|{"a": 1, "c": 2}|],

    "objectLTDueToValue" ~: "" ~: LT ~=?
        parseValueOrDie [r|{"a": 1, "b": 2}|] `compareValues`
        parseValueOrDie [r|{"a": 1, "c": 3}|],

    "emptyArrLTNonEmpty" ~: "" ~: LT ~=? parseValueOrDie "[]" `compareValues` parseValueOrDie "[1]",

    "nonEmptyArrGTEmpty" ~: "" ~: GT ~=? parseValueOrDie "[1]" `compareValues` parseValueOrDie "[]",

    "equalArrays" ~: "" ~: EQ ~=? parseValueOrDie "[1, 0, 3]" `compareValues`
        parseValueOrDie "[1, 0, 3]",

    "arrayLT" ~: "" ~: LT ~=? parseValueOrDie "[1, 0, 10]" `compareValues`
        parseValueOrDie "[1, 3, 3]",

    "stringIsGreaterThanInt" ~: "" ~: GT ~=?
        parseValueOrDie [r|"foo"|] `compareValues` parseValueOrDie "1",

    "intIsLessThanString" ~: "" ~: LT ~=?
        parseValueOrDie "1" `compareValues` parseValueOrDie [r|"foo"|]
    ]

extendedJSONTests :: Test
extendedJSONTests = TestList [
    -- TODO: Add support for non-integral numeric types in BSON.
    "cannotParseNonIntegralJSONNumber" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "1.23"),

    "canParseCanonicalExtendedJSONNumberInt" ~: "" ~: Right (IntValue 3) ~=?
        valueFromString [r|{"$numberInt": "3"}|],

    "parseCanonicalExtendedJSONFailsToParseWhenValueIsJSONNumber" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$numberInt": 3}|]),

    "parseCanonicalExtendedJSONFailsToParseWhenValueIsNonNumericString" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$numberInt": "foo"}|]),

    "parseCanonicalExtendedJSONFailsToParseWhenValueIsNotAnInteger" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$numberInt": "3.14"}|]),

    "parseCanonicalExtendedJSONFailsToParseWhenIntOverflowsPositive" ~: "" ~: Overflow ~=?
        getErrCode (valueFromString [r|{"$numberInt": "100000000000"}|]),

    "parseRelaxedExtendedJSONFailsToParseWhenIntOverflowsPositive" ~: "" ~: Overflow ~=?
        getErrCode (valueFromString "100000000000"),

    "parseCanonicalExtendedJSONFailsToParseWhenIntOverflowsNegative" ~: "" ~: Overflow ~=?
        getErrCode (valueFromString [r|{"$numberInt": "-100000000000"}|]),

    "parseRelaxedExtendedJSONFailsToParseWhenIntOverflowsNegative" ~: "" ~: Overflow ~=?
        getErrCode (valueFromString "-100000000000"),

    "parseCanonicalExtendedJSONFailsToParseWithExtraKey" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$numberInt": "3", "$extra": 3}|]),

    "canParseCanonicalExtendedJSONUndefined" ~: "" ~: Right UndefinedValue ~=?
        valueFromString [r|{"$undefined": true}|],

    "parseCanonicalExtendedJSONUndefinedFailsWhenValueIsFalse" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$undefined": false}|]),

    "parseCanonicalExtendedJSONUndefinedFailsWhenValueIsNumber" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$undefined": 3}|]),

    "parseCanonicalExtendedJSONUndefinedFailsWhenThereIsAnExtraKey" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString [r|{"$undefined": true, "$extra": true}|]),

    "unknownCanonicalJSONKeywordIsPermitted" ~: "" ~:
        Right (DocumentValue Document { getFields = [("$unknown", IntValue 3)] }) ~=?
        valueFromString [r|{"$unknown": 3}|],

    "canParseQueryPredicateToValue" ~: "" ~:
        Right (DocumentValue Document { getFields = [("foo",
            DocumentValue Document { getFields = [("$gt", IntValue 3)]})]}) ~=?
        valueFromString [r|{"foo": {"$gt": 3}}|],

    "canParseArrayContainingCanonicalExtendedJSONEncodings" ~: "" ~:
        Right (ArrayValue Array { getElements = [IntValue (-8), UndefinedValue] }) ~=?
        valueFromString [r|[{"$numberInt": "-8"}, {"$undefined": true}]|],

    "canParseObjectContainingCanonicalExtendedJSONEncodings" ~: "" ~:
        Right (DocumentValue Document {
            getFields = [("foo", IntValue (-8)), ("bar", UndefinedValue)] }) ~=?
        valueFromString [r|{"foo": {"$numberInt": "-8"}, "bar": {"$undefined": true}}|],

    "canSerializeToStringInRelaxedMode" ~: "" ~:
       [r|{"a":null,"b":{"$undefined":true},"c":42,"d":true,"e":[null,43]}|] ~=?
        valueToString (DocumentValue Document { getFields = [
            ("a", NullValue),
            ("b", UndefinedValue),
            ("c", IntValue 42),
            ("d", BoolValue True),
            ("e", ArrayValue Array { getElements = [NullValue, IntValue 43]})]}) Relaxed,

    "canSerializeToStringInCanonicalMode" ~: "" ~:
       filter (\x -> not (isSeparator x) && not (isSpace x)) [r|{
           "a":null,
           "b":{"$undefined":true},
           "c":{"$numberInt":"42"},
           "d":true,
           "e":[null,{"$numberInt":"43"}]
        }|] ~=?
        valueToString (DocumentValue Document { getFields = [
            ("a", NullValue),
            ("b", UndefinedValue),
            ("c", IntValue 42),
            ("d", BoolValue True),
            ("e", ArrayValue Array { getElements = [NullValue, IntValue 43]})]}) Canonical
    ]

valueTest :: Test
valueTest = TestList [
    basicTests,
    extendedJSONTests
    ]
