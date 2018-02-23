{-# LANGUAGE GADTs #-}

module Mongo.Expression (
    Expr(..),
    ) where

import Mongo.Bool3VL
import Mongo.Value

-- The expression part of the MQL. It is an abstract data structure with no particular concrete syntax.
data Expr a where
    -- Note that the first parameter is String not Expr (String). It means that the field names are fixed in a query text
    -- and cannot by computed during the runtime. We may want to revisit this assumption.
    SelectField::String->Expr Document->Expr Value
    -- Same applies to array indices
    SelectElem::Int->Expr Array->Expr Value

    -- If the field does not exist in the document then it is added to it, otherwise it is overwritten
    SetField::(String, Expr Value)->Expr Document->Expr Document
    RemoveField::String->Expr Document->Expr Document
    HasField::String->Expr Document->Expr Bool

    -- Selectors for the Value
    GetInt::Expr Value->Expr Int
    GetBool::Expr Value->Expr Bool
    GetString::Expr Value->Expr String
    GetArray::Expr Value->Expr Array
    GetDocument::Expr Value->Expr Document

    PutInt::Expr Int->Expr Value
    PutDocument::Expr Document ->Expr Value

    IsNull::Expr Value->Expr Bool

    -- Constant expression
    Const::Value->Expr Value

    -- Arithmetic
    Plus::Expr Int->Expr Int->Expr Int
    Minus::Expr Int->Expr Int->Expr Int

    -- Comparisons
    CompareEQ::Expr Value->Expr Value->Expr Bool
    CompareEQ3VL::Expr Value->Expr Value->Expr Bool3VL

    -- If <cond> <then expr> <else expr>
    If::Expr Bool->Expr Value->Expr Value->Expr Value

    -- Access a variable in the current environment, operators ("stages") set e.g. "$$root" et al.
    Var::String->Expr Value
