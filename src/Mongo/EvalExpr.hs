{-# LANGUAGE GADTs #-}

module Mongo.EvalExpr(
    evalExpr
    ) where

import Mongo.Expression
import Mongo.Variant

evalExpr::Expr a-> Maybe a
evalExpr (Const c) = return c

-- Selectors
evalExpr (SelectField f v) =
    evalExpr v >>= getField f

evalExpr (SelectElem i v) =
    evalExpr v >>= getElement i

evalExpr (SetField (f,v) d) =
    do
        doc <- evalExpr d
        val <- evalExpr v
        return $ addField (f,val) (removeField f doc)

evalExpr (RemoveField f v) =
    evalExpr v >>= return . removeField f

evalExpr (HasField f v) =
    do
        doc <- evalExpr v
        return (case getField f doc of
                    Just _ -> True
                    Nothing -> False)
    
evalExpr (GetInt v) =
    evalExpr v >>= getIntValue

evalExpr (GetBool v) =
    evalExpr v >>= getBoolValue

evalExpr (GetString v) =
    evalExpr v >>= getStringValue

evalExpr (GetArray v) =
    evalExpr v >>= getArrayValue
    
evalExpr (GetDocument v) =
    evalExpr v >>= getDocumentValue

evalExpr (PutInt v) =
    evalExpr v >>= return . IntValue

evalExpr (PutDocument v) =
    evalExpr v >>= return . DocumentValue

evalExpr (IsNull v) =
    evalExpr v >>= isNull

-- Arithmetic
evalExpr (Plus lhs rhs) =
    (+) <$> (evalExpr lhs) <*> (evalExpr rhs)

evalExpr (Minus lhs rhs) =
    (-) <$> (evalExpr lhs) <*> (evalExpr rhs)

-- Comparisons    
evalExpr (CompareEQ lhs rhs) =
    compareEQ <$> (evalExpr lhs) <*> (evalExpr rhs) 

evalExpr (CompareEQ3VL lhs rhs) =
    compareEQ3VL <$> (evalExpr lhs) <*> (evalExpr rhs) 

evalExpr (If cond t e) =
    do
        condval <- evalExpr cond
        if condval
        then
            evalExpr t
        else
            evalExpr e
