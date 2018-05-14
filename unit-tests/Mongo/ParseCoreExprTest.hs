{-# LANGUAGE QuasiQuotes #-}

module Mongo.ParseCoreExprTest(
    parseTest
    ) where

import Data.Char (isSeparator, isSpace)
import Mongo.CoreExpr
import Mongo.Error
import Mongo.ParseCoreExpr
import Mongo.Value
import Test.HUnit
import Text.RawString.QQ

testDocumentExpr = GetDocument (Const (DocumentValue (Document [])))
testArrayExpr = GetArray (Const (ArrayValue (Array [])))
testStringExpr = GetString (Const (StringValue "foo"))
testIntExpr = GetInt (Const (IntValue 5))
testBoolExpr = GetBool (Const (BoolValue True))
testValueExpr = Const (IntValue 5)

parseTest :: Test
parseTest = TestList [
    "roundtripConst" ~: Right (Const (IntValue 5)) ~=? parseP (coreExprToValue (Const (IntValue 5))),
    "roundtripConst2" ~: (parseP (parseValueOrDie [r|{"$const": 5}|])::Either Error (CoreExpr Int)) ~=? parseP (coreExprToValue (Const (IntValue 5))),

    "roundtripSelectField" ~: Right (SelectField testStringExpr testDocumentExpr) ~=? 
        parseP (coreExprToValue (SelectField testStringExpr testDocumentExpr)),

    "roundtripSelectElem" ~: Right (SelectElem testIntExpr testArrayExpr) ~=? 
        parseP (coreExprToValue (SelectElem testIntExpr testArrayExpr)),

    "roundtripSetField" ~: Right (SetField (testStringExpr, testValueExpr) testDocumentExpr) ~=? 
        parseP (coreExprToValue (SetField (testStringExpr, testValueExpr) testDocumentExpr)),

    "roundtripRemoveField" ~: Right (RemoveField testStringExpr testDocumentExpr) ~=? 
        parseP (coreExprToValue (RemoveField testStringExpr testDocumentExpr)),

    "roundtripHasField" ~: Right (HasField testStringExpr testDocumentExpr) ~=? 
        parseP (coreExprToValue (HasField testStringExpr testDocumentExpr)),

    "roundtripSetElem" ~: Right (SetElem (testIntExpr, testValueExpr) testArrayExpr) ~=? 
        parseP (coreExprToValue (SetElem (testIntExpr, testValueExpr) testArrayExpr)),

    "roundtripArrayLength" ~: Right (ArrayLength testArrayExpr) ~=? 
        parseP (coreExprToValue (ArrayLength testArrayExpr)),

    "roundtripGetInt" ~: Right (GetInt testValueExpr) ~=? 
        parseP (coreExprToValue (GetInt testValueExpr)),

    "roundtripGetBool" ~: Right (GetBool testValueExpr) ~=? 
        parseP (coreExprToValue (GetBool testValueExpr)),

    "roundtripGetString" ~: Right (GetString testValueExpr) ~=? 
        parseP (coreExprToValue (GetString testValueExpr)),
        
    "roundtripGetArray" ~: Right (GetArray testValueExpr) ~=? 
        parseP (coreExprToValue (GetArray testValueExpr)),

    "roundtripGetDocument" ~: Right (GetDocument testValueExpr) ~=? 
        parseP (coreExprToValue (GetDocument testValueExpr)),

    "roundtripPutInt" ~: Right (PutInt testIntExpr) ~=? 
        parseP (coreExprToValue (PutInt testIntExpr)),

    "roundtripPutBool" ~: Right (PutBool testBoolExpr) ~=? 
        parseP (coreExprToValue (PutBool testBoolExpr)),

    "roundtripPutString" ~: Right (PutString testStringExpr) ~=? 
        parseP (coreExprToValue (PutString testStringExpr)),
        
    "roundtripPutArray" ~: Right (PutArray testArrayExpr) ~=? 
        parseP (coreExprToValue (PutArray testArrayExpr)),

    "roundtripPutDocument" ~: Right (PutDocument testDocumentExpr) ~=? 
        parseP (coreExprToValue (PutDocument testDocumentExpr)),

    "roundtripIsNull" ~: Right (IsNull testValueExpr) ~=? 
        parseP (coreExprToValue (IsNull testValueExpr)),

    "roundtripIsUndefined" ~: Right (IsUndefined testValueExpr) ~=? 
        parseP (coreExprToValue (IsUndefined testValueExpr)),

    "roundtripIsInt" ~: Right (IsInt testValueExpr) ~=? 
        parseP (coreExprToValue (IsInt testValueExpr)),

    "roundtripIsBool" ~: Right (IsBool testValueExpr) ~=? 
        parseP (coreExprToValue (IsBool testValueExpr)),

    "roundtripIsString" ~: Right (IsString testValueExpr) ~=? 
        parseP (coreExprToValue (IsString testValueExpr)),
        
    "roundtripIsArray" ~: Right (IsArray testValueExpr) ~=? 
        parseP (coreExprToValue (IsArray testValueExpr)),

    "roundtripIsDocument" ~: Right (IsDocument testValueExpr) ~=? 
        parseP (coreExprToValue (IsDocument testValueExpr)),

    "roundtripAnd" ~: Right (And testBoolExpr testBoolExpr) ~=? 
        parseP (coreExprToValue (And testBoolExpr testBoolExpr)),

    "roundtripOr" ~: Right (Or testBoolExpr testBoolExpr) ~=? 
        parseP (coreExprToValue (Or testBoolExpr testBoolExpr)),

    "roundtripNot" ~: Right (Not testBoolExpr) ~=? 
        parseP (coreExprToValue (Not testBoolExpr)),

    "roundtripPlus" ~: Right (Plus testIntExpr testIntExpr) ~=? 
        parseP (coreExprToValue (Plus testIntExpr testIntExpr)),
        
    "roundtripMinus" ~: Right (Minus testIntExpr testIntExpr) ~=? 
        parseP (coreExprToValue (Minus testIntExpr testIntExpr)),
        
    "roundtripEq" ~: Right (CompareEQ testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (CompareEQ testValueExpr testValueExpr)),

    "roundtripLt" ~: Right (CompareLT testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (CompareLT testValueExpr testValueExpr)),
        
    "roundtripLte" ~: Right (CompareLTE testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (CompareLTE testValueExpr testValueExpr)),
        
    "roundtripGt" ~: Right (CompareGT testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (CompareGT testValueExpr testValueExpr)),
        
    "roundtripGte" ~: Right (CompareGTE testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (CompareGTE testValueExpr testValueExpr)),
        
    "roundtripIf" ~: Right (If testBoolExpr testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (If testBoolExpr testValueExpr testValueExpr)),
        
    "roundtripVar" ~: Right (Var "foo") ~=? 
        parseP (coreExprToValue (Var "foo")),

    "roundtripLet" ~: Right (Let "foo" testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (Let "foo" testValueExpr testValueExpr)),

    "roundtripFuncDef" ~: Right (FunctionDef "foo" (Function ["a","b"] testValueExpr) testValueExpr) ~=? 
        parseP (coreExprToValue (FunctionDef "foo" (Function ["a","b"] testValueExpr) testValueExpr)),

    "roundtripFuncApp" ~: Right (FunctionApp "foo" [testValueExpr,testValueExpr]) ~=? 
        parseP (coreExprToValue (FunctionApp "foo" [testValueExpr,testValueExpr])),

    "roundtripFoldValue" ~: Right (FoldValue "foo" testValueExpr testValueExpr) ~=? 
        parseP (coreExprToValue (FoldValue "foo" testValueExpr testValueExpr)),

    "roundtripFoldBool" ~: Right (FoldBool "foo" testBoolExpr testValueExpr) ~=? 
        parseP (coreExprToValue (FoldBool "foo" testBoolExpr testValueExpr))
    ]
