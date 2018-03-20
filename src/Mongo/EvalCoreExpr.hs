{-# LANGUAGE GADTs #-}

module Mongo.EvalCoreExpr(
    evalCoreExpr
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.Value

evalCoreExpr :: CoreExpr a -> Either Error a
evalCoreExpr (Const c) = return c

-- Selectors
evalCoreExpr (SelectField f v) =
    evalCoreExpr v >>= getField f

evalCoreExpr (SelectElem i v) =
    evalCoreExpr v >>= getElement i

evalCoreExpr (SetField (f,v) d) =
    do
        doc <- evalCoreExpr d
        val <- evalCoreExpr v
        return $ addField (f,val) (removeField f doc)

evalCoreExpr (RemoveField f v) =
    removeField f <$> evalCoreExpr v

evalCoreExpr (HasField f v) =
    hasField f <$> evalCoreExpr v

evalCoreExpr (GetInt v) =
    evalCoreExpr v >>= getIntValue

evalCoreExpr (GetBool v) =
    evalCoreExpr v >>= getBoolValue

evalCoreExpr (GetString v) =
    evalCoreExpr v >>= getStringValue

evalCoreExpr (GetArray v) =
    evalCoreExpr v >>= getArrayValue

evalCoreExpr (GetDocument v) =
    evalCoreExpr v >>= getDocumentValue

evalCoreExpr (PutInt v) =
    IntValue <$> evalCoreExpr v

evalCoreExpr (PutDocument v) =
    DocumentValue <$> evalCoreExpr v

evalCoreExpr (IsNull v) =
    isNull <$> evalCoreExpr v

-- Arithmetic
evalCoreExpr (Plus lhs rhs) =
    (+) <$> evalCoreExpr lhs <*> evalCoreExpr rhs

evalCoreExpr (Minus lhs rhs) =
    (-) <$> evalCoreExpr lhs <*> evalCoreExpr rhs

-- Comparisons
evalCoreExpr (CompareEQ lhs rhs) =
    compareEQ <$> evalCoreExpr lhs <*> evalCoreExpr rhs

evalCoreExpr (CompareEQ3VL lhs rhs) =
    compareEQ3VL <$> evalCoreExpr lhs <*> evalCoreExpr rhs

evalCoreExpr (If cond t e) =
    do
        condval <- evalCoreExpr cond
        if condval
        then
            evalCoreExpr t
        else
            evalCoreExpr e
