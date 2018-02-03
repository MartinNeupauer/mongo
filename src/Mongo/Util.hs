{-# LANGUAGE FlexibleInstances #-} 

module Mongo.Util (
    fromString,
    exprFromString,
    parseP,
    ) where

import Data.List
import Data.Ratio
import Mongo.Expression
import Mongo.Variant
import Text.JSON

fromTextJson::JSValue->Variant
fromTextJson json =
    case json of
        JSNull -> NullValue
        JSBool v -> BoolValue v
        JSRational false v -> case v of 
                                    x | denominator x == 1 -> IntValue $ fromIntegral $ numerator x
                                    _ -> NullValue
        JSString v -> StringValue $ fromJSString v
        JSArray v -> ArrayValue $ Array $ map (\x -> fromTextJson x) v
        JSObject v -> DocumentValue $ Document $ map (\x -> (fst x, fromTextJson $ snd x)) (fromJSObject v)

fromString::String->Maybe Variant
fromString input = 
    let decoded = decode input :: Result (JSObject JSValue) in

    case decoded of
        Ok val -> Just $ fromTextJson $ JSObject val
        _ -> Nothing

exprFromString::String->Maybe (Expr Variant)
exprFromString s = 
    (fromString s) >>= parseP


-- A quick and dirty parser from a JSON object to an expression
class Parseable a where
    parseP::Variant->Maybe (Expr a)
    parseP _ = Nothing

instance Parseable Int where
    parseP (DocumentValue d) =
        uncons (getFields d) >>= return . fst >>= (\(token,args) -> case token of
            "$const" -> return $ GetInt $ Const args
            "$getint" -> parseP args >>= return . GetInt
            "$plus" -> parsePlus args
            "$minus" -> parseMinus args
            _ -> Nothing
            )

instance Parseable Variant where
    parseP (DocumentValue d) =
        uncons (getFields d) >>= return . fst >>= (\(token,args) -> case token of
            "$const" -> return $ Const args
            "$field" -> parseSelectField args
            "$elem" -> parseSelectElem args
            "$putint" -> parseP args >>= return . PutInt
            "$putdocument" -> parseP args >>= return . PutDocument
            _ -> Nothing
            )

instance Parseable Document where
    parseP (DocumentValue d) =
        uncons (getFields d) >>= return . fst >>= (\(token,args) -> case token of
            "$const" -> return $ GetDocument $ Const args
            "$getdocument" -> parseP args >>= return . GetDocument
            "$set" -> parseSetField args
            "$remove" -> parseRemoveField args
            _ -> Nothing
            )

instance Parseable Array where
    parseP (DocumentValue d) =
        uncons (getFields d) >>= return . fst >>= (\(token,args) -> case token of
            "$const" -> return $ GetArray $ Const args
            "$getarray" -> parseP args >>= return . GetArray
            _ -> Nothing
            )

parsePlus params =
    do
        pa <- getArrayValue params
        lhs <- parseP (getElements pa !! 0)
        rhs <- parseP (getElements pa !! 1)
        return $ Plus lhs rhs

parseMinus params =
    do
        pa <- getArrayValue params
        lhs <- parseP (getElements pa !! 0)
        rhs <- parseP (getElements pa !! 1)
        return $ Minus lhs rhs

parseSelectField params =
    do
        pa <- getArrayValue params
        field <- getStringValue (getElements pa !! 0)
        doc <- parseP (getElements pa !! 1)
        return $ SelectField field doc

parseSelectElem params =
    do
        pa <- getArrayValue params
        elem <- getIntValue (getElements pa !! 0)
        array <- parseP (getElements pa !! 1)
        return $ SelectElem elem array
        
parseSetField params =
    do
        pa <- getArrayValue params
        field <- getStringValue (getElements pa !! 0)
        val <- parseP (getElements pa !! 1)
        doc <- parseP (getElements pa !! 2)
        return $ SetField (field,val) doc

parseRemoveField params =
    do
        pa <- getArrayValue params
        field <- getStringValue (getElements pa !! 0)
        doc <- parseP (getElements pa !! 1)
        return $ RemoveField field doc
        