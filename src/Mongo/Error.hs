module Mongo.Error(
    ErrorCode(..),
    Error(..)
    ) where

data ErrorCode
    = ArrayIndexOutOfBounds
    | FailedToParse
    | FunctionRedefinition
    | InvalidJSON
    | MissingField
    | NotImplemented
    | TypeMismatch
    | UnboundVariable
    | UnknownFunction
    | WrongNumberOfFunctionArgs
    deriving (Eq, Show)

data Error = Error { errCode :: ErrorCode, errReason :: String } deriving (Eq, Show)
