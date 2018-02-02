{-# LANGUAGE GADTs #-}

module Mongo.EvalExpr(
    evalExpr
    ) where

import Mongo.Expression
import Mongo.Variant

evalExpr::Expr a-> Maybe a
evalExpr (Const c) = Just c

-- Selectors
evalExpr (SelectField f v) =
    evalExpr v >>= getField f

evalExpr (SelectElem i v) =
    evalExpr v >>= getElement i

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
    evalExpr v >>= Just . IntValue

evalExpr (IsNull v) =
    evalExpr v >>= isNull

-- Arithmetic
evalExpr (Plus lhs rhs) =
    do
        x <- evalExpr lhs
        y <- evalExpr rhs
        return (x+y)

evalExpr (Minus lhs rhs) =
    do
        x <- evalExpr lhs
        y <- evalExpr rhs
        return (x-y)