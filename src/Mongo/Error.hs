module Mongo.Error(
    ErrorCode(..),
    Error(..),

    getErrCode,
    errorToString
    ) where

data ErrorCode
    = ArrayIndexOutOfBounds
    | FailedToParse
    | IllegalProjection
    | InvalidJSON
    | MissingField
    | NotImplemented
    | Overflow
    | TypeMismatch
    | UnboundVariable
    | UnknownFunction
    | WrongNumberOfFunctionArgs
    deriving (Eq, Show)

data Error = Error { errCode :: ErrorCode, errReason :: String } deriving (Eq, Show)

-- For testing, extracts the error code from an Either, or throws an exception if the Either does
-- not contain an error.
getErrCode :: Either Error a -> ErrorCode
getErrCode (Left err) = errCode err
getErrCode _ = error "Expected an error, but did not get one"

errorToString :: Error -> String
errorToString e = "Error code: " ++ show (errCode e) ++ " Error reason: " ++ errReason e 
