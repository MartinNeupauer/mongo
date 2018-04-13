module Mongo.MQLv1.MatchExpr(
    MatchExpr(..),
    MatchPredicate(..),

    evalMatchExpr,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value

-- The portion of a match expression which expresses a simple predicate over a Value, e.g. {$lt: 3}
-- or {$exists: true}.
data MatchPredicate
    -- Comparisons.
    = EqMatchExpr Value
    | LTEMatchExpr Value
    | LTMatchExpr Value
    | GTMatchExpr Value
    | GTEMatchExpr Value
    | NEMatchExpr Value

    -- This is $exists:true.
    --
    -- TODO: For now we assume that $exists:false is parsed to NotExpr (ExistsMatchExpr ..). In the
    -- future, we may wish to express $exists:false in terms of a flavor of match expression where
    -- all selected elements must match (and the document vacuously matches if there are no such
    -- elements). For the moment, however, match expressions always have "any selected element
    -- matches" semantics.
    | ExistsMatchExpr

    -- $elemMatch.
    | ElemMatchObjectExpr MatchExpr
    | ElemMatchValueExpr [MatchPredicate]

data MatchExpr
    -- Represents any predicate of the form {"path.to.match": {$example: <predicate>}}.
    = PathAcceptingExpr Path MatchPredicate

    -- Logical match expressions. The parser is responsible for converting $nor into $not -> $or.
    | AndMatchExpr [MatchExpr]
    | OrMatchExpr [MatchExpr]
    | NotMatchExpr MatchExpr

-- Given a flag indicating array traversal behavior, the name of a function to apply, and the name
-- of a bound variable containing a value:
-- * If there is no implicit array traversal, returns a core expr which applies the named function
--   to the value bound to the var.
-- * If there is implicit array traversal, returns a core expr which folds over the array.
makeApplyFuncExpr :: ImplicitArrayTraversal -> String -> String -> CoreExpr Value
makeApplyFuncExpr NoImplicitTraversal funcName retrievedVal = FunctionApp funcName [Var retrievedVal]

makeApplyFuncExpr ImplicitlyTraverseArrays funcName retrievedVal =
    If (IsArray (Var retrievedVal))
        (let foldFuncName = "fold_" ++ funcName in
            FunctionDef foldFuncName (Function ["v", "init"]
                (PutBool (Or (GetBool (Var "init")) (GetBool (FunctionApp funcName [Var "v"])))))
                (PutBool (FoldBool
                    foldFuncName
                    (GetBool (Const (BoolValue False)))
                    (Var retrievedVal))))
        (FunctionApp funcName [Var retrievedVal])

-- Generates a core expression which traverses the given path, and applies the named function to
-- each leaf element. The expression returns true if the named function ever returns true.
makeTraversePathExpr :: Path -> String -> CoreExpr Value
makeTraversePathExpr [] funcName = FunctionApp funcName [Var "ROOT"]

makeTraversePathExpr (PathComponent (FieldName f) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ f ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "value_of_" ++ f in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsDocument (Var formalParam)) (HasField f (GetDocument (Var formalParam))))
                (Let retrievedVal (SelectField f (GetDocument (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (ArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_" ++ show i in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem i (GetArray (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (FieldNameOrArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_or_field_" ++ show i in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem i (GetArray (Var formalParam)))
                    -- XXX: In MQLv1, there is no implicit array traversal when a path component
                    -- that can act as either an array index or a field name resolves to an array
                    -- index. That is, the document {a: {0: [1, 2, 3]}} matches the query
                    -- {"a.0": 3}, but the document {a: [[1, 2, 3]]} does not match.
                    (FunctionApp funcName [Var retrievedVal]))
                (If (And (IsDocument (Var formalParam))
                        (HasField (show i) (GetDocument (Var formalParam))))
                    (Let retrievedVal (SelectField (show i) (GetDocument (Var formalParam)))
                        (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                    (Const $ BoolValue False))))
            (makeTraversePathExpr rest nextLevelFuncName)

-- Helper for desugaring $lt, $lte, $gt, $gte, and $eq. The passed Function is the leaf predicate
-- that should be applied to each of the elements selected by the Path. The Value is the value which
-- the match expr is comparing against.
--
-- Returns the CoreExpr resulting from desugaring.
desugarComparisonMatchExpr :: Path -> Value -> Function -> CoreExpr Bool

-- XXX: Comparing to an array in MQLv1 matches if either any of the elements in the array matches,
-- or if the array as a whole matches. For example, {a: {$eq: [1, 2, 3]}} matches both the documents
-- {a: [1, 2, 3]} and {a: [0, [1, 2, 3]]}. The path selection behavior is therefore implicitly
-- encoded in the fact that this is an array comparison. Instead, users should probably have to say
-- explicitly "find me documents where 'a' is an array containing an element [1, 2, 3]" or "find me
-- documents where field 'a' contains exactly the array [1, 2, 3]".
desugarComparisonMatchExpr path (ArrayValue _) pred =
    GetBool $ FunctionDef "pred" pred (PutBool
        (Or (GetBool (makeTraversePathExpr (reverse path) "pred"))
            (GetBool (makeTraversePathExpr
                (reverse $ convertPathToNotTraverseTrailingArrays path) "pred"))))

desugarComparisonMatchExpr path val pred =
    GetBool $ FunctionDef "pred" pred (makeTraversePathExpr (reverse path) "pred")

-- XXX: Do we really want type bracketing going forward? The aggregation subsystem doesn't do it,
-- and this behavior is implicit and therefore non-obvious. We might want to make users explicitly
-- write type checks, or add new sugar which means "compare and also check type".
--
-- TODO: Type bracketing obeys the notion of "canonical type", but the model currently only supports
-- Value types whose canonical type codes are distinct. When we add two types which have the same
-- canonical type code (i.e. once we support both Int and Long), we will have to adjust the type
-- bracketing implementation to check canonical type.
makeTypeBracketingCheck :: Value -> CoreExpr Value -> CoreExpr Bool
makeTypeBracketingCheck NullValue expr = IsNull expr
makeTypeBracketingCheck UndefinedValue expr = IsUndefined expr
makeTypeBracketingCheck (IntValue _) expr = IsInt expr
makeTypeBracketingCheck (BoolValue _) expr = IsBool expr
makeTypeBracketingCheck (StringValue _) expr = IsString expr
makeTypeBracketingCheck (ArrayValue _) expr = IsArray expr
makeTypeBracketingCheck (DocumentValue _) expr = IsDocument expr

-- Returns a one-argument core language function which implements the match predicate. The function
-- accepts a value and returns a boolean indicating whether or not the value matches.
makeMatchPredicateFunction :: MatchPredicate -> Function
makeMatchPredicateFunction (EqMatchExpr val) =
    Function ["x"] (PutBool $ CompareEQ (Var "x") (Const val))

makeMatchPredicateFunction (LTMatchExpr val) =
    Function ["x"] (PutBool (And
        (CompareLT (Var "x") (Const val))
        (makeTypeBracketingCheck val (Var "x"))))

makeMatchPredicateFunction (LTEMatchExpr val) =
    Function ["x"] (PutBool (And
        (CompareLTE (Var "x") (Const val))
        (makeTypeBracketingCheck val (Var "x"))))

makeMatchPredicateFunction (GTMatchExpr val) =
    Function ["x"] (PutBool (And
        (CompareGT (Var "x") (Const val))
        (makeTypeBracketingCheck val (Var "x"))))

makeMatchPredicateFunction (GTEMatchExpr val) =
    Function ["x"] (PutBool (And
        (CompareGTE (Var "x") (Const val))
        (makeTypeBracketingCheck val (Var "x"))))

makeMatchPredicateFunction (NEMatchExpr val) =
    Function ["x"] (PutBool $ Not (CompareEQ (Var "x") (Const val)))

makeMatchPredicateFunction ExistsMatchExpr =
    Function ["x"] (Const $ BoolValue True)

makeMatchPredicateFunction (ElemMatchObjectExpr subexpr) =
    let desugaredSubExpr = desugarMatchExpr subexpr
        -- The value on which we're calling the desugared subexpression becomes the new root.
        subexprFunc = Function ["ROOT"] (PutBool desugaredSubExpr) in
        Function ["x"] (If (IsArray (Var "x"))
            (FunctionDef "runDesugaredElemMatch" subexprFunc
                (FunctionDef "foldFunc"
                    (Function ["v", "init"] (PutBool (Or
                        (GetBool (Var "init"))
                        (GetBool (FunctionApp "runDesugaredElemMatch" [Var "v"])))))
                    (PutBool (FoldBool "foldFunc"
                        (GetBool (Const (BoolValue False))) (Var "x")))))
            (Const $ BoolValue False))

makeMatchPredicateFunction (ElemMatchValueExpr preds) =
    let subFuncs = map makeMatchPredicateFunction preds
        -- Generate an expression which is the AND of applying all of the match predicate functions
        -- to the variable "x".
        applySubFuncsExpr = foldl
            (\expr f ->
                (And (GetBool (FunctionDef "subfunc" f (FunctionApp "subfunc" [Var "x"]))) expr))
            (GetBool $ Const (BoolValue True))
            subFuncs
        applySubFuncsFunc = Function ["x"] (PutBool applySubFuncsExpr)
    in Function ["x"] (If (IsArray (Var "x"))
        (FunctionDef "runDesugaredElemMatch" applySubFuncsFunc
            (FunctionDef "foldFunc"
                (Function ["v", "init"] (PutBool (Or
                    (GetBool (Var "init"))
                    (GetBool (FunctionApp "runDesugaredElemMatch" [Var "v"])))))
                (PutBool (FoldBool "foldFunc"
                    (GetBool (Const (BoolValue False))) (Var "x")))))
        (Const $ BoolValue False))

-- Compiles a MatchExpr which accepts a path into a core language expression.
desugarPathAcceptingExpr :: Path -> MatchPredicate -> CoreExpr Bool
desugarPathAcceptingExpr path (EqMatchExpr val) =
    desugarComparisonMatchExpr path val (makeMatchPredicateFunction (EqMatchExpr val))

desugarPathAcceptingExpr path (LTMatchExpr val) =
    desugarComparisonMatchExpr path val (makeMatchPredicateFunction (LTMatchExpr val))

desugarPathAcceptingExpr path (LTEMatchExpr val) =
    desugarComparisonMatchExpr path val (makeMatchPredicateFunction (LTEMatchExpr val))

desugarPathAcceptingExpr path (GTMatchExpr val) =
    desugarComparisonMatchExpr path val (makeMatchPredicateFunction (GTMatchExpr val))

desugarPathAcceptingExpr path (GTEMatchExpr val) =
    desugarComparisonMatchExpr path val (makeMatchPredicateFunction (GTEMatchExpr val))

desugarPathAcceptingExpr path (NEMatchExpr val) =
    Not $ desugarMatchExpr (PathAcceptingExpr path (EqMatchExpr val))

desugarPathAcceptingExpr path ExistsMatchExpr =
    GetBool $ FunctionDef "exists" (makeMatchPredicateFunction ExistsMatchExpr)
        (makeTraversePathExpr (reverse $ convertPathToNotTraverseTrailingArrays path) "exists")

desugarPathAcceptingExpr path (ElemMatchObjectExpr subexpr) =
    GetBool $ FunctionDef "elemMatchObj" (makeMatchPredicateFunction (ElemMatchObjectExpr subexpr))
        -- XXX: Right now $elemMatch isn't applied to each element of an array, e.g. the document
        -- {a: [[{b: 1, c: 1}]]} does not match the query {a: {$elemMatch: {b: 1, c: 1}}}. We should
        -- probably allow users to express array behavior, just as we would for any other match
        -- expression.
        (makeTraversePathExpr
            (reverse $ convertPathToNotTraverseTrailingArrays path) "elemMatchObj")

desugarPathAcceptingExpr path (ElemMatchValueExpr preds) =
    GetBool $ FunctionDef "elemMatchValue" (makeMatchPredicateFunction (ElemMatchValueExpr preds))
        -- XXX: Right now $elemMatch isn't applied to each element of an array, e.g. the document
        -- {a: [[1, 8]]} does not match the query {a: {$elemMatch: {$gt: 5, $lt: 10}}}. We should
        -- probably allow users to express array behavior, just as we would for any other match
        -- expression.
        (makeTraversePathExpr
            (reverse $ convertPathToNotTraverseTrailingArrays path) "elemMatchValue")

-- Compiles a MatchExpr into a core language expression.
desugarMatchExpr :: MatchExpr -> CoreExpr Bool

desugarMatchExpr (PathAcceptingExpr path pred) = desugarPathAcceptingExpr path pred

desugarMatchExpr (NotMatchExpr expr) = Not $ desugarMatchExpr expr

desugarMatchExpr (AndMatchExpr []) = GetBool $ Const (BoolValue True)
desugarMatchExpr (AndMatchExpr [hd]) = desugarMatchExpr hd
desugarMatchExpr (AndMatchExpr (hd:tail)) =
    And (desugarMatchExpr hd) (desugarMatchExpr (AndMatchExpr tail))

desugarMatchExpr (OrMatchExpr []) = GetBool $ Const (BoolValue False)
desugarMatchExpr (OrMatchExpr [hd]) = desugarMatchExpr hd
desugarMatchExpr (OrMatchExpr (hd:tail)) =
    Or (desugarMatchExpr hd) (desugarMatchExpr (OrMatchExpr tail))

-- Returns true if the Value matches the MatchExpr. Otherwise returns false. Any Error return value
-- is query-fatal.
evalMatchExpr :: MatchExpr -> Value -> Either Error Bool
evalMatchExpr matchExpr value =
    evalCoreExpr (desugarMatchExpr matchExpr)
        Environment { boundVariables = [("ROOT", value)], definedFunctions = [] }
