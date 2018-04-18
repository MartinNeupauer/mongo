module Mongo.ValueTest(
    valueTest
    ) where

import Mongo.Error
import Mongo.Value
import Test.HUnit

getErrCode :: Either Error a -> ErrorCode
getErrCode (Left err) = errCode err
getErrCode _ = error "Expected an error, but did not get one"

basicTests :: Test
basicTests = TestList [
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

extendedJSONTests :: Test
extendedJSONTests = TestList [
    -- TODO: Add support for non-integral numeric types in BSON.
    "cannotParseNonIntegralJSONNumber" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "1.23"),

    "canParseCanonicalExtendedJSONNumberInt" ~: "" ~: Right (IntValue 3) ~=?
        valueFromString "{\"$numberInt\": 3}",

    "parseCanonicalExtendedJSONFailsToParseWhenValueIsString" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$numberInt\": \"foo\"}"),

    "parseCanonicalExtendedJSONFailsToParseWithExtraKey" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$numberInt\": 3, \"$extra\": 3}"),

    "canParseCanonicalExtendedJSONUndefined" ~: "" ~: Right UndefinedValue ~=?
        valueFromString "{\"$undefined\": true}",

    "parseCanonicalExtendedJSONUndefinedFailsWhenValueIsFalse" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$undefined\": false}"),

    "parseCanonicalExtendedJSONUndefinedFailsWhenValueIsNumber" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$undefined\": 3}"),

    "parseCanonicalExtendedJSONUndefinedFailsWhenThereIsAnExtraKey" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$undefined\": true, \"$extra\": true}"),

    -- XXX: This behavior prevents us from encoding objects that have $-prefixed field names. We
    -- want the query language to have full expressivity over all valid BSON documents, regardless
    -- of the contents of their key names, so this restriction should be eliminated.
    "unknownCanonicalJSONKeywordResultsInError" ~: "" ~: FailedToParse ~=?
        getErrCode (valueFromString "{\"$unknown\": 3}"),

    "canParseArrayContainingCanonicalExtendedJSONEncodings" ~: "" ~:
        Right (ArrayValue Array { getElements = [IntValue (-8), UndefinedValue] }) ~=?
        valueFromString "[{\"$numberInt\": -8}, {\"$undefined\": true}]",

    "canParseObjectContainingCanonicalExtendedJSONEncodings" ~: "" ~:
        Right (DocumentValue Document {
            getFields = [("foo", IntValue (-8)), ("bar", UndefinedValue)] }) ~=?
        valueFromString "{\"foo\": {\"$numberInt\": -8}, \"bar\": {\"$undefined\": true}}"
    ]

valueTest :: Test
valueTest = TestList [
    basicTests,
    extendedJSONTests
    ]
