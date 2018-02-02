module Mongo.Variant (
    Variant(..),
    Array(..),
    Document(..),

    getIntValue,
    getBoolValue,
    getStringValue,
    getArrayValue,
    getDocumentValue,

    isNull,
    
    addField,
    getField,
    removeField,

    addElement,
    getElement,
    removeElement,
    ) where

import Data.List
import Data.Monoid

-- Basic data structure that holds values. It should model BSON more closely (i.e. various int types)
-- but it will do for now.
data Variant
    = NullValue
    | IntValue Int
    | BoolValue Bool
    | StringValue String
    | ArrayValue Array
    | DocumentValue Document
    deriving (Eq, Show)

-- Variant selectors
getIntValue (IntValue i) = Just i
getIntValue _ = Nothing

getBoolValue (BoolValue b) = Just b
getBoolValue _ = Nothing

getStringValue (StringValue s) = Just s
getStringValue _ = Nothing

getArrayValue (ArrayValue a) = Just a
getArrayValue _ = Nothing

getDocumentValue (DocumentValue d) = Just d
getDocumentValue _ = Nothing

isNull (NullValue) = Just True
isNull _  = Just False

-- The Array and Document types are not particularly efficient now. We don't care much as it's an
-- implementation detail and we are building a model not the best implementation of the model.
-- TODO: Change the kind from * to *->* so they can be made instances of Functor and Applicative.
newtype Array = Array { getArray::[Variant] } deriving (Eq, Show)
newtype Document = Document { getDocument::[(String, Variant)] } deriving (Eq, Show)

instance Monoid Array where
    mempty = Array []
    mappend lhs rhs = Array $ getArray lhs ++ getArray rhs

instance Monoid Document where
    mempty = Document []
    mappend lhs rhs = Document $ getDocument lhs ++ getDocument rhs

-- Does not check for duplicate field names
addField::(String,Variant)->Document->Document
addField field (Document document) = Document $ document ++ [field] 

getField::String->Document->Maybe Variant
getField field (Document document) = lookup field document

removeField::String->Document->Document
removeField field (Document document) = Document $ deleteBy (\lhs rhs -> fst lhs == fst rhs) (field, NullValue) document

addElement::Variant->Array->Array
addElement value (Array array) = Array $ array ++ [value]

getElement::Int->Array->Maybe Variant
getElement index (Array array) =
    if (index >=0 && index < length array)
    then
        Just (array !! index)
    else
        Nothing

deleteNth n xs = let (a, b) = splitAt n xs in a ++ tail b

removeElement::Int->Array->Array
removeElement index (Array array) =
    if (index >=0 && index < length array)
        then
            Array $ deleteNth index array            
        else
            Array array
        