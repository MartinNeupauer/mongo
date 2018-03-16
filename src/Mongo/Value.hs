module Mongo.Value (
    Value(..),
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
    hasField,
    removeField,

    addElement,
    getElement,
    removeElement,

    compareEQ,
    compareEQ3VL,
    ) where

import Data.List
import Data.Monoid
import Mongo.Bool3VL
import Mongo.Error
import qualified Data.Maybe

-- Basic data structure that holds values. It should model BSON more closely (i.e. various int types)
-- but it will do for now.
data Value
    = NullValue
    | UndefinedValue
    | IntValue Int
    | BoolValue Bool
    | StringValue String
    | ArrayValue Array
    | DocumentValue Document
    deriving (Eq, Show)

-- Value selectors
getIntValue :: Value -> Either Error Int
getIntValue (IntValue i) = Right i
getIntValue val = Left $
    Error { errCode = TypeMismatch, errReason = "Expected Int but found: " ++ (show val) }

getBoolValue :: Value -> Either Error Bool
getBoolValue (BoolValue b) = Right b
getBoolValue val = Left $
    Error { errCode = TypeMismatch, errReason = "Expected Bool but found: " ++ (show val) }

getStringValue :: Value -> Either Error String
getStringValue (StringValue s) = Right s
getStringValue val = Left $
    Error { errCode = TypeMismatch, errReason = "Expected String but found: " ++ (show val) }

getArrayValue :: Value -> Either Error Array
getArrayValue (ArrayValue a) = Right a
getArrayValue val = Left $
    Error { errCode = TypeMismatch, errReason = "Expected Array but found: " ++ (show val) }

getDocumentValue :: Value -> Either Error Document
getDocumentValue (DocumentValue d) = Right d
getDocumentValue val = Left $
    Error { errCode = TypeMismatch, errReason = "Expected Document but found: " ++ (show val) }

isNull :: Value -> Bool
isNull (NullValue) = True
isNull _  = False

-- The Array and Document types are not particularly efficient now. We don't care much as it's an
-- implementation detail and we are building a model not the best implementation of the model.
-- TODO: Change the kind from * to *->* so they can be made instances of Functor and Applicative.
newtype Array = Array { getElements::[Value] } deriving (Eq, Show)
newtype Document = Document { getFields::[(String, Value)] } deriving (Eq, Show)

instance Monoid Array where
    mempty = Array []
    mappend lhs rhs = Array $ getElements lhs ++ getElements rhs

instance Monoid Document where
    mempty = Document []
    mappend lhs rhs = Document $ getFields lhs ++ getFields rhs

-- Does not check for duplicate field names
addField::(String, Value)->Document->Document
addField field (Document document) = Document $ document ++ [field]

getField :: String -> Document -> Either Error Value
getField field (Document document) = case (lookup field document) of
    (Just val) -> Right val
    _ -> Left $ Error { errCode = MissingField, errReason = "Field " ++ field ++ " not found" }

hasField :: String -> Document -> Bool
hasField field (Document document) = Data.Maybe.isJust (lookup field document)

removeField::String->Document->Document
removeField field (Document document) = Document $ deleteBy (\lhs rhs -> fst lhs == fst rhs) (field, NullValue) document

addElement::Value->Array->Array
addElement value (Array array) = Array $ array ++ [value]

getElement :: Int -> Array -> Either Error Value
getElement index (Array array)
    | index < 0 = Left $ Error {
        errCode = ArrayIndexOutOfBounds,
        errReason = "Negative array index " ++ (show index) }
    | index >= arrLen = Left $ Error {
        errCode = ArrayIndexOutOfBounds,
        errReason = "Array index "
                    ++ (show index)
                    ++ " out of bounds. Array length: "
                    ++ (show arrLen) }
    | otherwise = Right (array !! index)
    where arrLen = (length array)

deleteNth n xs = let (a, b) = splitAt n xs in a ++ tail b

removeElement::Int->Array->Array
removeElement index (Array array) =
    if (index >=0 && index < length array)
        then
            Array $ deleteNth index array
        else
            Array array


compareEQ::Value->Value->Bool
compareEQ (NullValue) (NullValue) = True
compareEQ (IntValue lhs) (IntValue rhs) = lhs == rhs
compareEQ (BoolValue lhs) (BoolValue rhs) = lhs == rhs
compareEQ (StringValue lhs) (StringValue rhs) = lhs == rhs -- lexicographical comparison, ignores collation
compareEQ (ArrayValue lhs) (ArrayValue rhs) = lhs == rhs -- elementwise comparison
compareEQ (DocumentValue lhs) (DocumentValue rhs) = 
    ((sortBy compareFieldNames . getFields) lhs) == ((sortBy compareFieldNames . getFields) rhs) -- elementwise comparison
compareEQ _ _ = False

compareEQ3VL::Value->Value->Bool3VL
-- Anything compared to NULL is unknown
compareEQ3VL (NullValue) _ = Unknown3VL
compareEQ3VL _ (NullValue) = Unknown3VL
compareEQ3VL (IntValue lhs) (IntValue rhs) = convertTo3VL $ lhs == rhs
compareEQ3VL (BoolValue lhs) (BoolValue rhs) = convertTo3VL $ lhs == rhs
compareEQ3VL (StringValue lhs) (StringValue rhs) = convertTo3VL $ lhs == rhs -- lexicographical comparison, ignores collation
compareEQ3VL (ArrayValue lhs) (ArrayValue rhs) = compareArrayEQ (getElements lhs) (getElements rhs)
compareEQ3VL (DocumentValue lhs) (DocumentValue rhs) = 
    compareDocumentEQ ((sortBy compareFieldNames . getFields) lhs) ((sortBy compareFieldNames . getFields) rhs)
compareEQ3VL _ _ = False3VL

compareFieldNames lhs rhs = compare (fst lhs) (fst rhs)

-- Array comparison helper
compareArrayEQ::[Value]->[Value]->Bool3VL
compareArrayEQ [] [] = True3VL
compareArrayEQ [] (_:_) = False3VL
compareArrayEQ (_:_) [] = False3VL
compareArrayEQ (x:xs) (y:ys) = and3VL (compareEQ3VL x y) (compareArrayEQ xs ys)

-- Document comparison helper
-- Note: assumes same order of field names
compareDocumentEQ::[(String,Value)]->[(String,Value)]->Bool3VL
compareDocumentEQ [] [] = True3VL
compareDocumentEQ [] (_:_) = False3VL
compareDocumentEQ (_:_) [] = False3VL
compareDocumentEQ (x:xs) (y:ys) =
    if ( (fst x) /= (fst y) )
    then
        False3VL
    else    
        and3VL (compareEQ3VL (snd x) (snd y)) (compareDocumentEQ xs ys)
