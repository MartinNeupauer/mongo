module Mongo.MQLv1.MatchExpr(
    MatchExpr(..),

    evalMatchExpr,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value

data MatchExpr
    = EqMatchExpr Path Value
    | LTEMatchExpr Path Value
    | LTMatchExpr Path Value
    | GTMatchExpr Path Value
    | GTEMatchExpr Path Value

-- TODO: Implement.
desugarMatchExpr :: MatchExpr -> CoreExpr Bool
desugarMatchExpr _ = GetBool (Const $ BoolValue True)

evalMatchExpr :: MatchExpr -> Value -> Either Error Bool
evalMatchExpr matchExpr value =
    evalCoreExpr (desugarMatchExpr matchExpr) [("ROOT", value)]
