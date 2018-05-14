{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}

module Mongo.ParseCoreExpr (
    coreExprFromString,
    coreExprToValue,
    parseP,
    ) where

import Data.List
import Mongo.CoreExpr
import Mongo.Error
import Mongo.Value

exprToDocHelper tag args = DocumentValue (Document [(tag, ArrayValue (Array args))])

functionToValue::Function -> Value
functionToValue (Function args body) =
    DocumentValue (Document[("$args",
        ArrayValue (Array (map StringValue args) )), ("$body", coreExprToValue body)])

coreExprToValue::CoreExpr a -> Value
coreExprToValue (Const c) = DocumentValue (Document [("$const",c)])
coreExprToValue (SelectField f d) = exprToDocHelper "$field" [coreExprToValue f, coreExprToValue d]
coreExprToValue (SelectElem i a) = exprToDocHelper "$elem" [coreExprToValue i, coreExprToValue a]
coreExprToValue (SetField (f,v) d) =
    exprToDocHelper "$set" [coreExprToValue f, coreExprToValue v, coreExprToValue d]
coreExprToValue (RemoveField f d) = exprToDocHelper "$remove" [coreExprToValue f, coreExprToValue d]
coreExprToValue (HasField f d) = exprToDocHelper "$hasField" [coreExprToValue f, coreExprToValue d]
coreExprToValue (SetElem (i,v) a) =
    exprToDocHelper "$set"[coreExprToValue i, coreExprToValue v, coreExprToValue a]
coreExprToValue (ArrayLength a) = DocumentValue (Document[("$length", coreExprToValue a)])

coreExprToValue (GetInt v) = DocumentValue (Document [("$getInt", coreExprToValue v)])
coreExprToValue (GetBool v) = DocumentValue (Document [("$getBool", coreExprToValue v)])
coreExprToValue (GetString v) = DocumentValue (Document [("$getString", coreExprToValue v)])
coreExprToValue (GetArray v) = DocumentValue (Document [("$getArray", coreExprToValue v)])
coreExprToValue (GetDocument v) = DocumentValue (Document [("$getDocument", coreExprToValue v)])

coreExprToValue (PutInt v) = DocumentValue (Document [("$putInt", coreExprToValue v)])
coreExprToValue (PutBool v) = DocumentValue (Document [("$putBool", coreExprToValue v)])
coreExprToValue (PutString v) = DocumentValue (Document [("$putString", coreExprToValue v)])
coreExprToValue (PutArray v) = DocumentValue (Document [("$putArray", coreExprToValue v)])
coreExprToValue (PutDocument v) = DocumentValue (Document [("$putDocument", coreExprToValue v)])

coreExprToValue (IsNull v) = DocumentValue (Document [("$isNull", coreExprToValue v)])
coreExprToValue (IsUndefined v) = DocumentValue (Document [("$isUndefined", coreExprToValue v)])
coreExprToValue (IsInt v) = DocumentValue (Document [("$isInt", coreExprToValue v)])
coreExprToValue (IsBool v) = DocumentValue (Document [("$isBool", coreExprToValue v)])
coreExprToValue (IsString v) = DocumentValue (Document [("$isString", coreExprToValue v)])
coreExprToValue (IsArray v) = DocumentValue (Document [("$isArray", coreExprToValue v)])
coreExprToValue (IsDocument v) = DocumentValue (Document [("$isDocument", coreExprToValue v)])

coreExprToValue (And l r) = exprToDocHelper "$and" [coreExprToValue l, coreExprToValue r]
coreExprToValue (Or l r) = exprToDocHelper "$or" [coreExprToValue l, coreExprToValue r]
coreExprToValue (Not v) = DocumentValue (Document [("$not", coreExprToValue v)])

coreExprToValue (Plus l r) = exprToDocHelper "$plus" [coreExprToValue l, coreExprToValue r]
coreExprToValue (Minus l r) = exprToDocHelper "$minus" [coreExprToValue l, coreExprToValue r]

coreExprToValue (CompareEQ l r) = exprToDocHelper "$eq" [coreExprToValue l, coreExprToValue r]
coreExprToValue (CompareLT l r) = exprToDocHelper "$lt" [coreExprToValue l, coreExprToValue r]
coreExprToValue (CompareLTE l r) = exprToDocHelper "$lte" [coreExprToValue l, coreExprToValue r]
coreExprToValue (CompareGT l r) = exprToDocHelper "$gt" [coreExprToValue l, coreExprToValue r]
coreExprToValue (CompareGTE l r) = exprToDocHelper "$gte" [coreExprToValue l, coreExprToValue r]

coreExprToValue (If c t e) =
    exprToDocHelper "$if" [coreExprToValue c, coreExprToValue t, coreExprToValue e]
coreExprToValue (Var v) = DocumentValue (Document [("$var", StringValue v)])
coreExprToValue (Let n e i) =
    exprToDocHelper "$let" [StringValue n, coreExprToValue e, coreExprToValue i]
coreExprToValue (FunctionDef n f i) =
    exprToDocHelper "$funcDef" [StringValue n, functionToValue f, coreExprToValue i]
coreExprToValue (FunctionApp n a) =
    exprToDocHelper "$funcApp" [StringValue n, ArrayValue (Array (map coreExprToValue a))]

coreExprToValue (FoldValue n i v) =
    exprToDocHelper "$fold" [StringValue n, coreExprToValue i, coreExprToValue v]
coreExprToValue (FoldBool n i v) =
    exprToDocHelper "$fold" [StringValue n, coreExprToValue i, coreExprToValue v]

coreExprFromString :: String -> Either Error (CoreExpr Value)
coreExprFromString s =
    valueFromString s >>= parseP

-- A quick and dirty parser from a JSON object to a core expression.
class Parseable a where
    parseP :: Value -> Either Error (CoreExpr a)
    parseP _ = Left Error { errCode = NotImplemented, errReason = "Expression not parseable" }

instance Parseable Int where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning Int, but found empty document"
            }
            ("$const",args):_ -> return $ GetInt $ Const args
            ("$getInt",args):_ -> GetInt <$> parseP args
            ("$length",args):_ -> ArrayLength <$> parseP args
            ("$plus",args):_ -> parsePlus args
            ("$minus",args):_ -> parseMinus args
            (token, _):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning Int: " ++ token
            }

instance Parseable Bool where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning Bool, but found empty document"
            }
            ("$const",args):_ -> return $ GetBool $ Const args
            ("$getBool",args):_ -> GetBool <$> parseP args
            ("$hasField",args):_ -> parseHasField args
            ("$isNull",args):_ -> IsNull <$> parseP args
            ("$isUndefined",args):_ -> IsUndefined <$> parseP args
            ("$isInt",args):_ -> IsInt <$> parseP args
            ("$isBool",args):_ -> IsBool <$> parseP args
            ("$isString",args):_ -> IsString <$> parseP args
            ("$isArray",args):_ -> IsArray <$> parseP args
            ("$isDocument",args):_ -> IsDocument <$> parseP args
            ("$and",args):_ -> parseAnd args
            ("$or",args):_ -> parseOr args
            ("$not",args):_ -> Not <$> parseP args
            ("$eq",args):_ -> parseEq args
            ("$lt",args):_ -> parseLt args
            ("$lte",args):_ -> parseLte args
            ("$gt",args):_ -> parseGt args
            ("$gte",args):_ -> parseGte args
            ("$fold",args):_ -> parseFoldBool args
            (token,_):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning Bool: " ++ token
            }

instance Parseable String where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning String, but found empty document"
            }
            ("$const",args):_ -> return $ GetString $ Const args
            ("$getString",args):_ -> GetString <$> parseP args
            -- TODO: handle string expressions ($concat, $toUpper/Lower, etc).
            (token,_):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning String: " ++ token
            }

instance Parseable Value where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning Value, but found empty document"
            }
            ("$const",args):_ -> return $ Const args
            ("$field",args):_ -> parseSelectField args
            ("$elem",args):_ -> parseSelectElem args
            ("$if",args):_ -> parseIf args
            ("$putBool",args):_ -> PutBool <$> parseP args
            ("$putInt",args):_ -> PutInt <$> parseP args
            ("$putString",args):_ -> PutString <$> parseP args
            ("$putArray",args):_ -> PutArray <$> parseP args
            ("$putDocument",args):_ -> PutDocument <$> parseP args
            ("$var",args):_ -> Var <$> getStringValue args
            ("$let",args):_ -> parseLet args
            ("$funcDef",args):_ -> parseFuncDef args
            ("$funcApp",args):_ -> parseFuncApp args
            ("$fold",args):_ -> parseFoldValue args
            (token,_):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning Value: " ++ token
            }

instance Parseable Document where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning Document, but found empty document"
            }
            ("$const",args):_ -> return $ GetDocument $ Const args
            ("$getDocument",args):_ -> GetDocument <$> parseP args
            ("$set",args):_ -> parseSetField args
            ("$remove",args):_ -> parseRemoveField args
            (token,_):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning Document: " ++ token
            }

instance Parseable Array where
    parseP (DocumentValue d) =
        case getFields d of
            [] -> Left Error {
                errCode = FailedToParse,
                errReason = "Expected expression returning Array, but found empty document"
            }
            ("$const",args):_ -> return $ GetArray $ Const args
            ("$getArray",args):_ -> GetArray <$> parseP args
            ("$set",args):_ -> parseSetElem args
            (token,_):_ -> Left Error {
                errCode = FailedToParse,
                errReason = "Unknown expression returning Array: " ++ token
            }

parseBinaryOp operator params =
    do
        pa <- getArrayValue params
        lhs <- parseP (getElements pa !! 0)
        rhs <- parseP (getElements pa !! 1)
        return $ operator lhs rhs

parseAnd = parseBinaryOp And
parseOr = parseBinaryOp Or
parsePlus = parseBinaryOp Plus
parseMinus = parseBinaryOp Minus
parseEq = parseBinaryOp CompareEQ
parseLt = parseBinaryOp CompareLT
parseLte = parseBinaryOp CompareLTE
parseGt = parseBinaryOp CompareGT
parseGte = parseBinaryOp CompareGTE

parseFieldOp operator params =
    do
        pa <- getArrayValue params
        field <- parseP (getElements pa !! 0)
        doc <- parseP (getElements pa !! 1)
        return $ operator field doc

parseSelectField = parseFieldOp SelectField
parseRemoveField = parseFieldOp RemoveField
parseHasField = parseFieldOp HasField

parseSelectElem params =
    do
        pa <- getArrayValue params
        elem <- parseP (getElements pa !! 0)
        array <- parseP (getElements pa !! 1)
        return $ SelectElem elem array
        
parseSetField params =
    do
        pa <- getArrayValue params
        field <- parseP (getElements pa !! 0)
        val <- parseP (getElements pa !! 1)
        doc <- parseP (getElements pa !! 2)
        return $ SetField (field,val) doc

parseSetElem params =
    do
        pa <- getArrayValue params
        idx <- parseP (getElements pa !! 0)
        val <- parseP (getElements pa !! 1)
        arr <- parseP (getElements pa !! 2)
        return $ SetElem (idx,val) arr

parseIf params =
    do
        pa <- getArrayValue params
        cond <- parseP (getElements pa !! 0)
        t <- parseP (getElements pa !! 1)
        e <- parseP (getElements pa !! 2)
        return $ If cond t e

parseLet params =
    do
        pa <- getArrayValue params
        name <- getStringValue (getElements pa !! 0)
        e <- parseP (getElements pa !! 1)
        i <- parseP (getElements pa !! 2)
        return $ Let name e i

parseFunction :: Value -> Either Error Function
parseFunction (DocumentValue f) = 
    do
        args <- getField "$args" f >>= getArrayValue >>= traverse getStringValue . getElements 
        body <- getField "$body" f >>= parseP::Either Error (CoreExpr Value) 
        return $ Function args body

parseFuncDef params =
    do
        pa <- getArrayValue params
        name <- getStringValue (getElements pa !! 0)
        f <- parseFunction (getElements pa !! 1)
        i <- parseP (getElements pa !! 2)
        return $ FunctionDef name f i

parseFuncApp params =
    do
        pa <- getArrayValue params
        name <- getStringValue (getElements pa !! 0)
        args <- getArrayValue (getElements pa !! 1) >>= traverse parseP . getElements
        return $ FunctionApp name args

parseFold op params =
    do
        pa <- getArrayValue params
        name <- getStringValue (getElements pa !! 0)
        i <- parseP (getElements pa !! 1)
        v <- parseP (getElements pa !! 2)
        return $ op name i v
                
parseFoldBool = parseFold FoldBool
parseFoldValue = parseFold FoldValue
