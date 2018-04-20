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
    isUndefined,
    isInt,
    isBool,
    isString,
    isArray,
    isDocument,

    addField,
    getField,
    hasField,
    removeField,

    arrayLength,

    addElement,
    getElement,
    removeElement,

    compareEQ3VL,

    compareEQ,
    compareLT,
    compareLTE,
    compareGT,
    compareGTE,
    compareValues,

    parseValueOrDie,
    valueFromString,
    valueFromTextJson,
    ) where

import Data.Int (Int32)
import Data.List
import Data.Monoid
import Data.Ratio
import Mongo.Bool3VL
import Mongo.Error
import qualified Data.Maybe
import qualified Text.JSON as JSON

-- Basic data structure that holds values. It should model BSON more closely (i.e. various int types)
-- but it will do for now.
data Value
    = NullValue
    -- The deprecated Undefined BSON type (0x06). It is here for completeness. While undefined and
    -- null probably shouldn't be separate values, this is useful for formalizing how the language
    -- is supposed to behave for the Undefined values that already exist in the wild.
    | UndefinedValue
    -- TODO WRITING-2753: We should use Int32 rather than a regular Int, since BSON int is a 32 bit
    -- signed integer.
    | IntValue Int
    | BoolValue Bool
    | StringValue String
    | ArrayValue Array
    | DocumentValue Document
    deriving (Eq, Show)

-- The Array and Document types are not particularly efficient now. We don't care much as it's an
-- implementation detail and we are building a model not the best implementation of the model.
-- TODO: Change the kind from * to *->* so they can be made instances of Functor and Applicative.
newtype Array = Array { getElements::[Value] } deriving (Eq, Show)
newtype Document = Document { getFields::[(String, Value)] } deriving (Eq, Show)

-- Value selectors
getIntValue :: Value -> Either Error Int
getIntValue (IntValue i) = Right i
getIntValue val = Left
    Error { errCode = TypeMismatch, errReason = "Expected Int but found: " ++ show val }

getBoolValue :: Value -> Either Error Bool
getBoolValue (BoolValue b) = Right b
getBoolValue val = Left
    Error { errCode = TypeMismatch, errReason = "Expected Bool but found: " ++ show val }

getStringValue :: Value -> Either Error String
getStringValue (StringValue s) = Right s
getStringValue val = Left
    Error { errCode = TypeMismatch, errReason = "Expected String but found: " ++ show val }

getArrayValue :: Value -> Either Error Array
getArrayValue (ArrayValue a) = Right a
getArrayValue val = Left
    Error { errCode = TypeMismatch, errReason = "Expected Array but found: " ++ show val }

getDocumentValue :: Value -> Either Error Document
getDocumentValue (DocumentValue d) = Right d
getDocumentValue val = Left
    Error { errCode = TypeMismatch, errReason = "Expected Document but found: " ++ show val }

isNull :: Value -> Bool
isNull NullValue = True
isNull _  = False

isUndefined :: Value -> Bool
isUndefined UndefinedValue = True
isUndefined _  = False

isInt :: Value -> Bool
isInt (IntValue _) = True
isInt _  = False

isBool :: Value -> Bool
isBool (BoolValue _) = True
isBool _  = False

isString :: Value -> Bool
isString (StringValue _) = True
isString _  = False

isArray :: Value -> Bool
isArray (ArrayValue _) = True
isArray _ = False

isDocument :: Value -> Bool
isDocument (DocumentValue _) = True
isDocument _ = False

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
getField field (Document document) = case lookup field document of
    (Just val) -> Right val
    _ -> Left Error { errCode = MissingField, errReason = "Field " ++ field ++ " not found" }

hasField :: String -> Document -> Bool
hasField field (Document document) = Data.Maybe.isJust (lookup field document)

removeField::String->Document->Document
removeField field (Document document) = Document $ deleteBy (\lhs rhs -> fst lhs == fst rhs) (field, NullValue) document

addElement::Value->Array->Array
addElement value (Array array) = Array $ array ++ [value]

arrayLength :: Array -> Int
arrayLength Array { getElements = arr } = length arr

getElement :: Int -> Array -> Either Error Value
getElement index (Array array)
    | index < 0 = Left Error {
        errCode = ArrayIndexOutOfBounds,
        errReason = "Negative array index " ++ show index }
    | index >= arrLen = Left Error {
        errCode = ArrayIndexOutOfBounds,
        errReason = "Array index "
                    ++ show index
                    ++ " out of bounds. Array length: "
                    ++ show arrLen }
    | otherwise = Right (array !! index)
    where arrLen = length array

deleteNth n xs = let (a, b) = splitAt n xs in a ++ tail b

removeElement::Int->Array->Array
removeElement index (Array array) =
    if index >=0 && index < length array
        then
            Array $ deleteNth index array
        else
            Array array

-- Maps from a Value to a numerical type code which is valid for use in Value comparison. These
-- codes have no semantic value, other than that they establish an ordering of types.
canonicalTypeCode :: Value -> Int
canonicalTypeCode UndefinedValue = 1
canonicalTypeCode NullValue = 2
canonicalTypeCode (IntValue _) = 3
canonicalTypeCode (StringValue _) = 4
canonicalTypeCode (DocumentValue _) = 5
canonicalTypeCode (ArrayValue _) = 6
canonicalTypeCode (BoolValue _) = 7

compareValues :: Value -> Value -> Ordering
compareValues UndefinedValue UndefinedValue = EQ
compareValues NullValue NullValue = EQ
compareValues (IntValue lhs) (IntValue rhs) = lhs `compare` rhs
compareValues (StringValue lhs) (StringValue rhs) = lhs `compare` rhs

compareValues (DocumentValue Document {getFields=[]}) (DocumentValue Document {getFields=[]}) = EQ
compareValues (DocumentValue Document {getFields=_}) (DocumentValue Document {getFields=[]}) = GT
compareValues (DocumentValue Document {getFields=[]}) (DocumentValue Document {getFields=_}) = LT
compareValues (DocumentValue Document { getFields = (lhsField, lhsValue) : lhsRest })
    (DocumentValue Document { getFields = (rhsField, rhsValue) : rhsRest }) =
    let fieldComparison = lhsField `compare` rhsField
        valueComparison = lhsValue `compareValues` rhsValue in
        case (fieldComparison, valueComparison) of
            (EQ, EQ) -> DocumentValue Document { getFields = lhsRest }
                `compareValues` DocumentValue Document { getFields = rhsRest }
            (LT, _) -> LT
            (GT, _) -> GT
            (_, valueCmp) -> valueCmp

compareValues (ArrayValue Array {getElements=[]}) (ArrayValue Array {getElements=[]}) = EQ
compareValues (ArrayValue Array {getElements=_}) (ArrayValue Array {getElements=[]}) = GT
compareValues (ArrayValue Array {getElements=[]}) (ArrayValue Array {getElements=_}) = LT
compareValues (ArrayValue Array { getElements = lhsFirst : lhsRest })
    (ArrayValue Array { getElements = rhsFirst : rhsRest })
    | cmp == EQ = ArrayValue Array { getElements = lhsRest } `compareValues`
        ArrayValue Array { getElements = rhsRest }
    | otherwise = cmp
    where cmp = lhsFirst `compareValues` rhsFirst

compareValues (BoolValue lhs) (BoolValue rhs) = lhs `compare` rhs

compareValues lhs rhs = canonicalTypeCode lhs `compare` canonicalTypeCode rhs

compareEQ :: Value -> Value -> Bool
compareEQ lhs rhs = (lhs `compareValues` rhs) == EQ

compareLT :: Value -> Value -> Bool
compareLT lhs rhs = (lhs `compareValues` rhs) == LT

compareLTE :: Value -> Value -> Bool
compareLTE lhs rhs = (lhs `compareValues` rhs) `elem` [LT, EQ]

compareGT :: Value -> Value -> Bool
compareGT lhs rhs = (lhs `compareValues` rhs) == GT

compareGTE :: Value -> Value -> Bool
compareGTE lhs rhs = (lhs `compareValues` rhs) `elem` [GT, EQ]

compareEQ3VL::Value->Value->Bool3VL
-- Anything compared to NULL is unknown
compareEQ3VL NullValue _ = Unknown3VL
compareEQ3VL _ NullValue = Unknown3VL
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
    if fst x /= fst y
    then
        False3VL
    else
        and3VL (compareEQ3VL (snd x) (snd y)) (compareDocumentEQ xs ys)

checkForIntOverflow :: Int -> Either Error Int
checkForIntOverflow myInt =
    if myInt == (fromIntegral (fromIntegral myInt :: Int32) :: Int)
    then Right myInt
    else Left Error { errCode = Overflow, errReason = "Integer overflow: " ++ show myInt }

parseStringToInt :: String -> Either Error Int
parseStringToInt str = case reads str of
    [(res, "")] -> checkForIntOverflow res
    _ -> Left Error {
        errCode = FailedToParse,
        errReason = "Could not parse string to int: " ++ str }

decodeJSONNumber :: Rational -> Either Error Value
decodeJSONNumber x
    | denominator x == 1 = checkForIntOverflow (fromIntegral $ numerator x) >>= (Right . IntValue)
    | otherwise = Left Error {
        errCode = FailedToParse,
        errReason = "Non-integral JSON numbers not yet supported" }

-- Decodes a JSON object, represented as a list of (string, JSON value) pairs, into a Value. Since
-- we support the extended JSON format, the JSON object could represent any Value (not just a
-- Document).
--
-- XXX: Extended JSON type wrapper objects which are not recognized just get parsed like normal
-- objects. This permits extended JSON to be used to encode queries (so long as there are no naming
-- conflicts between MQL and extended JSON). Instead, extended JSON should probably be improved so
-- that it has an escaping mechanism for representing objects with $-prefixed keys. Furthermore, we
-- might want to consider a special character other than "$" in extended JSON, so that it becomes
-- easier to encode MQL queries.
--
-- Returns an error if the object is not valid extended JSON.
decodeJSONObj :: [(String, JSON.JSValue)] -> Either Error Value

decodeJSONObj [("$numberInt", JSON.JSString s)] =
    parseStringToInt (JSON.fromJSString s) >>= (Right . IntValue)
decodeJSONObj (("$numberInt", _):_) = Left Error {
    errCode = FailedToParse,
    errReason = "could not parse $numberInt" }

decodeJSONObj [("$undefined", JSON.JSBool True)] = Right UndefinedValue
decodeJSONObj (("$undefined", _):_) = Left Error {
    errCode = FailedToParse,
    errReason = "could not parse $undefined" }

decodeJSONObj obj = mapM (\x -> do
    subVal <- valueFromTextJson $ snd x
    Right (fst x, subVal)) obj
        >>= (Right . DocumentValue . Document)

valueFromTextJson :: JSON.JSValue -> Either Error Value
valueFromTextJson json =
    case json of
        JSON.JSNull -> Right NullValue
        JSON.JSBool v -> Right (BoolValue v)
        JSON.JSRational false v -> decodeJSONNumber v
        JSON.JSString v -> Right $ StringValue $ JSON.fromJSString v
        JSON.JSArray v -> mapM valueFromTextJson v >>= (Right . ArrayValue . Array)
        JSON.JSObject v -> decodeJSONObj (JSON.fromJSObject v)

-- Parses an extended JSON 2.0.0 string to a Value.
--
-- See https://github.com/mongodb/specifications/blob/master/source/extended-json.rst.
valueFromString :: String -> Either Error Value
valueFromString input =
    case JSON.decode input of
        JSON.Ok val -> valueFromTextJson val
        JSON.Error jsonErrString -> Left Error {
            errCode = InvalidJSON,
            errReason = "JSON failed to parse: " ++ jsonErrString }

-- Parses a JSON string to a value, or throws a fatal exception if parsing fails. Useful for
-- testing.
parseValueOrDie :: String -> Value
parseValueOrDie str = case valueFromString str of
    Right v -> v
    Left Error { errCode = code, errReason = reason } -> error reason
