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
    SelectField::CoreExpr String->CoreExpr Document->CoreExpr Value
    SelectElem::CoreExpr Int->CoreExpr Array->CoreExpr Value

    -- If the field does not exist in the document then it is added to it, otherwise it is overwritten
    SetField::(CoreExpr String, CoreExpr Value)->CoreExpr Document->CoreExpr Document
    RemoveField::CoreExpr String->CoreExpr Document->CoreExpr Document
    HasField::CoreExpr String->CoreExpr Document->CoreExpr Bool

    -- When needed, the array is extended with NullValue until i <= the length of the array.
    SetElem::(CoreExpr Int, CoreExpr Value)->CoreExpr Array->CoreExpr Array

    ArrayLength :: CoreExpr Array -> CoreExpr Int

    -- Selectors for the Value.
    GetInt::CoreExpr Value->CoreExpr Int
    GetBool::CoreExpr Value->CoreExpr Bool
    GetString::CoreExpr Value->CoreExpr String
    GetArray::CoreExpr Value->CoreExpr Array
    GetDocument::CoreExpr Value->CoreExpr Document

    PutBool :: CoreExpr Bool -> CoreExpr Value
    PutInt::CoreExpr Int->CoreExpr Value
    PutArray::CoreExpr Array->CoreExpr Value
    PutDocument::CoreExpr Document ->CoreExpr Value

    -- Type predicates.
    IsNull :: CoreExpr Value -> CoreExpr Bool
    IsUndefined :: CoreExpr Value -> CoreExpr Bool
    IsInt :: CoreExpr Value -> CoreExpr Bool
    IsBool :: CoreExpr Value -> CoreExpr Bool
    IsString :: CoreExpr Value -> CoreExpr Bool
    IsArray :: CoreExpr Value -> CoreExpr Bool
    IsDocument :: CoreExpr Value -> CoreExpr Bool

    -- Logical.
    And :: CoreExpr Bool -> CoreExpr Bool -> CoreExpr Bool
    Or :: CoreExpr Bool -> CoreExpr Bool -> CoreExpr Bool
    Not :: CoreExpr Bool -> CoreExpr Bool

    -- Constant expression
    Const::Value->CoreExpr Value

    -- Arithmetic
    Plus::CoreExpr Int->CoreExpr Int->CoreExpr Int
    Minus::CoreExpr Int->CoreExpr Int->CoreExpr Int

    -- Comparisons
    -- TODO: Avoid polymorphic comparisons?
    CompareEQ :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool
    CompareLT :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool
    CompareLTE :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool
    CompareGT :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool
    CompareGTE :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool
    CompareEQ3VL :: CoreExpr Value -> CoreExpr Value -> CoreExpr Bool3VL

    -- If <cond> <then expr> <else expr>
    If::CoreExpr Bool->CoreExpr Value->CoreExpr Value->CoreExpr Value

    -- Access a variable in the current environment, operators ("stages") set e.g. "$$ROOT" et al.
    Var::String->CoreExpr Value

    -- Bind a variable in the current environment.
    Let :: String -> CoreExpr Value -> CoreExpr Value -> CoreExpr Value

    -- A function definition consists of the function name, a list of formal arguments, the function
    -- body, and the remainder of the expression. Functions are available only within the scope of
    -- their subexpression, and function names can be shadowed (just like variables in Let
    -- expressions).
    --
    -- However, functions do not support closures. When a function is called, the only in-scope
    -- variables are the bindings for the function's formal parameters.
    FunctionDef :: String -> Function -> CoreExpr Value -> CoreExpr Value

    -- Function application. Consists of the name of the function being applied and a list of
    -- expressions whose resulting values will get bound to the formal parameters of the function.
    -- All arguments are evaluated eagerly.
    FunctionApp :: String -> [CoreExpr Value] -> CoreExpr Value

    -- An expression for traversing a Value and returning another Value. The traversal is
    -- single-level, *not* recursive.
    --
    -- Arguments are:
    -- * The name of a two-argument function which returns a Value. The first argument is one of the
    --   Values being traversed, and the second is the accumulator.
    -- * A sub-expression which returns the initial value of the fold.
    -- * A sub-expression which returns the value to fold over.
    --
    -- Similar to the traditional fold from functional languages. All values are considered
    -- traversable. Scalars can be "traversed" as through they were one-element lists.
    --
    -- Useful for determining whether all Values in an Array or Document match some predicate, or
    -- whether any Value in an Array or Document matches some predicate.
    FoldValue :: String -> CoreExpr Value -> CoreExpr Value -> CoreExpr Value

    -- A variant of FoldValue where the return value is a Bool.
    --
    -- Useful for determining whether all Values in an Array or Document match some predicate, or
    -- whether any Value in an Array or Document matches some predicate.
    FoldBool :: String -> CoreExpr Bool -> CoreExpr Value -> CoreExpr Bool
