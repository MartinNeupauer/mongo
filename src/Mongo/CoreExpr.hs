{-# LANGUAGE GADTs #-}

module Mongo.CoreExpr (
    CoreExpr(..),
    Function(..),
    ) where

import Mongo.Bool3VL
import Mongo.Value

-- A function consists of a list of formal parameter names, and a body.
data Function = Function [String] (CoreExpr Value)

-- The core expression language. It is an abstract data structure with no particular concrete
-- syntax.  This language should be small and stable. User-facing pieces of MQL such as match
-- expressions, agg expressions, projection, and updates should be implementable in terms of the
-- core language.
data CoreExpr a where
    -- Note that the first parameter is String not CoreExpr (String). It means that the field names
    -- are fixed in a query text and cannot by computed during the runtime. We may want to revisit
    -- this assumption.
    SelectField::String->CoreExpr Document->CoreExpr Value
    -- Same applies to array indices
    SelectElem::Int->CoreExpr Array->CoreExpr Value

    -- If the field does not exist in the document then it is added to it, otherwise it is overwritten
    SetField::(String, CoreExpr Value)->CoreExpr Document->CoreExpr Document
    RemoveField::String->CoreExpr Document->CoreExpr Document
    HasField::String->CoreExpr Document->CoreExpr Bool

    -- Selectors for the Value
    GetInt::CoreExpr Value->CoreExpr Int
    GetBool::CoreExpr Value->CoreExpr Bool
    GetString::CoreExpr Value->CoreExpr String
    GetArray::CoreExpr Value->CoreExpr Array
    GetDocument::CoreExpr Value->CoreExpr Document

    PutBool :: CoreExpr Bool -> CoreExpr Value
    PutInt::CoreExpr Int->CoreExpr Value
    PutDocument::CoreExpr Document ->CoreExpr Value

    IsNull::CoreExpr Value->CoreExpr Bool

    -- Constant expression
    Const::Value->CoreExpr Value

    -- Arithmetic
    Plus::CoreExpr Int->CoreExpr Int->CoreExpr Int
    Minus::CoreExpr Int->CoreExpr Int->CoreExpr Int

    -- Comparisons
    CompareEQ::CoreExpr Value->CoreExpr Value->CoreExpr Bool
    CompareEQ3VL::CoreExpr Value->CoreExpr Value->CoreExpr Bool3VL

    -- If <cond> <then expr> <else expr>
    If::CoreExpr Bool->CoreExpr Value->CoreExpr Value->CoreExpr Value

    -- Access a variable in the current environment, operators ("stages") set e.g. "$$ROOT" et al.
    Var::String->CoreExpr Value

    -- Bind a variable in the current environment.
    Let :: String -> CoreExpr Value -> CoreExpr Value -> CoreExpr Value

    -- A function definition consists of the function name, a list of formal arguments, the function
    -- body, and the remainder of the expression. Functions have global scope and closures are not
    -- supported (the only bound variables in the body of a function are its arguments).
    FunctionDef :: String -> Function -> CoreExpr Value -> CoreExpr Value

    -- Function application. Consists of the name of the function being applied and a list of
    -- expressions whose resulting values will get bound to the formal parameters of the function.
    -- All arguments are evaluated eagerly.
    FunctionApp :: String -> [CoreExpr Value] -> CoreExpr Value
