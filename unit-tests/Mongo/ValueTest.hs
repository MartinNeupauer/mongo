module Mongo.ValueTest(
    valueTest
    ) where

import Mongo.Value
import Test.HUnit

parseNullScalar = TestCase (assertEqual "" (Right NullValue) (valueFromString "null"))

parseIntValue = TestCase (assertEqual "" (Right $ IntValue 345) (valueFromString "345"))

parseTrueValue = TestCase (assertEqual "" (Right $ BoolValue True) (valueFromString "true"))

parseFalseValue = TestCase (assertEqual "" (Right $ BoolValue False) (valueFromString "false"))

parseStringValue = TestCase (assertEqual "" (Right $ StringValue "foo") (valueFromString "\"foo\""))

parseArrayValue = TestCase (assertEqual ""
    (Right $ ArrayValue Array { getElements = [IntValue 1, IntValue 2] })
    (valueFromString "[1, 2]"))

parseDocumentValue = TestCase (assertEqual ""
    (Right $ DocumentValue Document { getFields = [
        ("a", NullValue),
        ("b", ArrayValue Array { getElements = [] }),
        ("c", DocumentValue Document { getFields = [] }),
        ("d", BoolValue True)
        ] })
    (valueFromString "{\"a\": null, \"b\": [], \"c\": {}, \"d\": true}"))

valueTest :: Test
valueTest = TestList [
    TestLabel "parseNullScalar" parseNullScalar,
    TestLabel "parseIntValue" parseIntValue,
    TestLabel "parseTrueValue" parseTrueValue,
    TestLabel "parseFalseValue" parseFalseValue,
    TestLabel "parseArrayValue" parseArrayValue,
    TestLabel "parseDocumentValue" parseDocumentValue
    ]
