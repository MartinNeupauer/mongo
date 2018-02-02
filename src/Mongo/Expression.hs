{-# LANGUAGE GADTs #-}

module Mongo.Expression (
    Expr(..)
    ) where

import Mongo.Variant

-- The expression part of the MQL. It is an abstract data structure with no particular concrete syntax.
-- TODO: - possible replace Maybe with Either or Result
data Expr a where
    -- Note that the first parameter is String not Expr (Maybe String). It means that the field names are fixed in a query text
    -- and cannot by computed during the runtime. We may want to revisit this assumption.
    SelectField::String->Expr (Maybe Document)->Expr (Maybe Variant)
    -- Same applies to array indices
    SelectElem::Int->Expr (Maybe Array)->Expr (Maybe Variant)

    -- Selectors for the Variant
    GetInt::Expr (Maybe Variant)->Expr (Maybe Int)
    GetBool::Expr (Maybe Variant)->Expr (Maybe Bool)
    GetString::Expr (Maybe Variant)->Expr (Maybe String)
    GetArray::Expr (Maybe Variant)->Expr (Maybe Array)
    GetDocument::Expr (Maybe Variant)->Expr (Maybe Document)

    PutInt::Expr (Maybe Int)->Expr (Maybe Variant)
    
    IsNull::Expr (Maybe Variant)->Expr (Maybe Bool)

    -- Constant expression
    Const::Variant->Expr (Maybe Variant)

    -- Arithmetic
    Plus::Expr (Maybe Int)->Expr (Maybe Int)->Expr (Maybe Int)
    Minus::Expr (Maybe Int)->Expr (Maybe Int)->Expr (Maybe Int)