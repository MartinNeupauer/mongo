module Mongo.Error(
    ErrorCode(..),
    Error(..)
    ) where

data ErrorCode
    = ArrayIndexOutOfBounds
    | FailedToParse
    | InvalidJSON
    | MissingField
    | NotImplemented
    | TypeMismatch
    deriving (Eq, Show)

data Error = Error { errCode :: ErrorCode, errReason :: String } deriving (Eq, Show)
