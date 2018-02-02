{-# LANGUAGE FlexibleInstances #-} 

module Mongo.Util (
    fromString,
    exprFromJSON,

    ) where

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

class Parseable a where
    parseP::Variant->Maybe (Expr a)

parsePlus params =
    do
        pa <- getArrayValue params
        lhs <- exprFromJSON (getArray pa !! 0)
        rhs <- exprFromJSON (getArray pa !! 1)
        return (PutInt (Plus (GetInt lhs) (GetInt rhs)))

exprFromJSON input =
    case input of
        (DocumentValue d) ->
            let fields = getDocument d
            in 
                if length fields == 0
                then
                    Nothing
                else
                    if (fst . head) fields == "$const"
                    then
                        Just $ Const ((snd . head) fields)
                    else 
                    if (fst . head) fields == "$plus"
                    then
                        parsePlus $ (snd . head) fields
                    else
                        Nothing
        _ -> Nothing
