{-# LANGUAGE GADTs #-}

module Mongo.EvalCoreExpr(
    evalCoreExpr
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.Value

-- An environment maps a variable name to an associated bound value.
type Environment = [(String, Value)]

evalCoreExpr :: CoreExpr a -> Environment -> Either Error a
evalCoreExpr (Const c) _ = return c

-- Selectors
evalCoreExpr (SelectField f v) env =
    evalCoreExpr v env >>= getField f

evalCoreExpr (SelectElem i v) env =
    evalCoreExpr v env >>= getElement i

evalCoreExpr (SetField (f,v) d) env =
    do
        doc <- evalCoreExpr d env
        val <- evalCoreExpr v env
        return $ addField (f,val) (removeField f doc)

evalCoreExpr (RemoveField f v) env =
    removeField f <$> evalCoreExpr v env

evalCoreExpr (HasField f v) env =
    hasField f <$> evalCoreExpr v env

evalCoreExpr (GetInt v) env =
    evalCoreExpr v env >>= getIntValue

evalCoreExpr (GetBool v) env =
    evalCoreExpr v env >>= getBoolValue

evalCoreExpr (GetString v) env =
    evalCoreExpr v env >>= getStringValue

evalCoreExpr (GetArray v) env =
    evalCoreExpr v env >>= getArrayValue

evalCoreExpr (GetDocument v) env =
    evalCoreExpr v env >>= getDocumentValue

evalCoreExpr (PutInt v) env =
    IntValue <$> evalCoreExpr v env

evalCoreExpr (PutDocument v) env =
    DocumentValue <$> evalCoreExpr v env

evalCoreExpr (IsNull v) env =
    isNull <$> evalCoreExpr v env

-- Arithmetic
evalCoreExpr (Plus lhs rhs) env =
    (+) <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (Minus lhs rhs) env =
    (-) <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

-- Comparisons
evalCoreExpr (CompareEQ lhs rhs) env =
    compareEQ <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareEQ3VL lhs rhs) env =
    compareEQ3VL <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (If cond t e) env =
    do
        condval <- evalCoreExpr cond env
        if condval
        then
            evalCoreExpr t env
        else
            evalCoreExpr e env

-- Extract the value of a variable from the environment.
evalCoreExpr (Var var) env = case lookup var env of
    Just v -> Right v
    _ -> Left Error { errCode = UnboundVariable, errReason = "Unbound variable: " ++ var }
