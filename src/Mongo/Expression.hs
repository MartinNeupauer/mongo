{-# LANGUAGE GADTs #-}

module Mongo.Expression (
    Expr(..),
    ) where

import Mongo.Bool3VL
import Mongo.Variant

-- The expression part of the MQL. It is an abstract data structure with no particular concrete syntax.
data Expr a where
    -- Note that the first parameter is String not Expr (String). It means that the field names are fixed in a query text
    -- and cannot by computed during the runtime. We may want to revisit this assumption.
    SelectField::String->Expr Document->Expr Variant
    -- Same applies to array indices
    SelectElem::Int->Expr Array->Expr Variant

    SetField::(String, Expr Variant)->Expr Document->Expr Document
    RemoveField::String->Expr Document->Expr Document

    -- Selectors for the Variant
    GetInt::Expr Variant->Expr Int
    GetBool::Expr Variant->Expr Bool
    GetString::Expr Variant->Expr String
    GetArray::Expr Variant->Expr Array
    GetDocument::Expr Variant->Expr Document

    PutInt::Expr Int->Expr Variant
    PutDocument::Expr Document ->Expr Variant

    IsNull::Expr Variant->Expr Bool

    -- Constant expression
    Const::Variant->Expr Variant

    -- Arithmetic
    Plus::Expr Int->Expr Int->Expr Int
    Minus::Expr Int->Expr Int->Expr Int

    -- Comparisons
    CompareEQ::Expr Variant->Expr Variant->Expr Bool
    CompareEQ3VL::Expr Variant->Expr Variant->Expr Bool3VL

