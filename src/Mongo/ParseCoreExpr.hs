{-# LANGUAGE FlexibleInstances #-}

module Mongo.ParseCoreExpr (
    coreExprFromString,
    parseP,
    ) where

import Data.List
import Mongo.CoreExpr
import Mongo.Error
import Mongo.Value

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
            ("$eq",args):_ -> parseEq args
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
            ("$putint",args):_ -> PutInt <$> parseP args
            ("$putdocument",args):_ -> PutDocument <$> parseP args
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
            ("$getdocument",args):_ -> GetDocument <$> parseP args
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
            ("$getarray",args):_ -> GetArray <$> parseP args
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

parsePlus = parseBinaryOp Plus
parseMinus = parseBinaryOp Minus
parseEq = parseBinaryOp CompareEQ

parseFieldOp operator params =
    do
        pa <- getArrayValue params
        field <- parseP (getElements pa !! 0)
        doc <- parseP (getElements pa !! 1)
        return $ operator field doc

parseSelectField = parseFieldOp SelectField
parseRemoveField = parseFieldOp RemoveField

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

parseIf params =
    do
        pa <- getArrayValue params
        cond <- parseP (getElements pa !! 0)
        t <- parseP (getElements pa !! 1)
        e <- parseP (getElements pa !! 2)
        return $ If cond t e
