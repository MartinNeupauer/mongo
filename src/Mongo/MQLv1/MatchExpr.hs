{-# LANGUAGE LambdaCase #-}

module Mongo.MQLv1.MatchExpr(
    MatchExpr(..),
    MatchPredicate(..),

    evalMatchExpr,
    evalJsonMatchExpr,
    evalStringMatchExpr,
    parseMatchExprJson,
    parseMatchExprString,
    ) where

import Data.Char (isDigit)
import Data.List.Split
import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value
import qualified Text.JSON as JSON

-- The portion of a match expression which expresses a simple predicate over a Value, e.g. {$lt: 3}
-- or {$exists: true}.
data MatchPredicate
    -- Comparisons.
    = EqMatchExpr Value
    | LTEMatchExpr Value
    | LTMatchExpr Value
    | GTMatchExpr Value
    | GTEMatchExpr Value

    -- XXX: This is quite subtle. While {a: {$not: {$gt: 0, $lt: 0}}} is logically more like
    -- {$not: {a: {$gt: 0, $lt: 0}}}, and should be rewritten into a top-level NotMatchExpr, $not
    -- behaves differently in the context of $elemMatch value. Consider the following example:
    --
    -- {a: {$elemMatch: {$not: {$gt: 0, $lt: 0}}}
    --
    -- Here, $not acts as a modifier to the predicate that gets applied to each array element, as
    -- opposed to acting as the logical negation of an entire MatchExpr.
    | NotMatchPred MatchPredicate
    | AndMatchPred [MatchPredicate]

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
    | ElemMatchValueExpr MatchPredicate
    deriving (Eq, Show)

data MatchExpr
    -- Represents any predicate of the form {"path.to.match": {$example: <predicate>}}.
    = PathAcceptingExpr Path MatchPredicate

    -- Logical match expressions. The parser is responsible for converting $nor into $not -> $or.
    | AndMatchExpr [MatchExpr]
    | OrMatchExpr [MatchExpr]
    | NotMatchExpr MatchExpr
    deriving (Eq, Show)

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
        retrievedVal = "value_of_" ++ f
        field = GetString (Const (StringValue f)) in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsDocument (Var formalParam)) (HasField field (GetDocument (Var formalParam))))
                (Let retrievedVal (SelectField field (GetDocument (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (ArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_" ++ show i
        index = GetInt (Const (IntValue i)) in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem index (GetArray (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (FieldNameOrArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_or_field_" ++ show i
        field = GetString (Const (StringValue (show i)))
        index = GetInt (Const (IntValue i)) in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem index (GetArray (Var formalParam)))
                    -- XXX: In MQLv1, there is no implicit array traversal when a path component
                    -- that can act as either an array index or a field name resolves to an array
                    -- index. That is, the document {a: {0: [1, 2, 3]}} matches the query
                    -- {"a.0": 3}, but the document {a: [[1, 2, 3]]} does not match.
                    (FunctionApp funcName [Var retrievedVal]))
                (If (And (IsDocument (Var formalParam))
                        (HasField field (GetDocument (Var formalParam))))
                    (Let retrievedVal (SelectField field (GetDocument (Var formalParam)))
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

makeMatchPredicateFunction (NotMatchPred pred) =
    let subfunc = makeMatchPredicateFunction pred in
        Function ["x"] (FunctionDef "subfunc" subfunc
            (PutBool $ Not $ GetBool (FunctionApp "subfunc" [Var "x"])))

makeMatchPredicateFunction (AndMatchPred subPreds) =
    let subFuncs = map makeMatchPredicateFunction subPreds
        -- Generate an expression which is the AND of applying all of the match predicate functions
        -- to the variable "x".
        applySubFuncsExpr = foldl
            (\expr f ->
                (And (GetBool (FunctionDef "subfunc" f (FunctionApp "subfunc" [Var "x"]))) expr))
            (GetBool $ Const (BoolValue True))
            subFuncs in
                Function ["x"] (PutBool applySubFuncsExpr)

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

makeMatchPredicateFunction (ElemMatchValueExpr pred) =
    let subfunc = makeMatchPredicateFunction pred
        applySubfuncFunc = Function ["x"]
            (FunctionDef "subfunc" subfunc (FunctionApp "subfunc" [Var "x"]))
    in Function ["x"] (If (IsArray (Var "x"))
        (FunctionDef "runDesugaredElemMatch" applySubfuncFunc
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

desugarPathAcceptingExpr path (NotMatchPred pred) =
    Not $ desugarMatchExpr (PathAcceptingExpr path pred)

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

desugarPathAcceptingExpr path (ElemMatchValueExpr pred) =
    GetBool $ FunctionDef "elemMatchValue" (makeMatchPredicateFunction (ElemMatchValueExpr pred))
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

-- Parses the contents of a $not. The input Document is the "body" in {... $not: { <body> }, ... }.
parseNotBody :: Document -> Either Error MatchPredicate
parseNotBody Document { getFields = [] } =
    Left Error { errCode = FailedToParse, errReason = "$not cannot contain an empty document" }

parseNotBody Document { getFields = subexprs } =
    do
        matchPreds <- mapM parseMatchPredicate subexprs
        Right $ AndMatchPred matchPreds

-- Parses a match expression leaf predicate such as $eq, $lt, $exists, and so on.
parseMatchPredicate :: (String, Value) -> Either Error MatchPredicate
parseMatchPredicate ("$eq", v) = Right $ EqMatchExpr v
parseMatchPredicate ("$lt", v) = Right $ LTMatchExpr v
parseMatchPredicate ("$lte", v) = Right $ LTEMatchExpr v
parseMatchPredicate ("$gt", v) = Right $ GTMatchExpr v
parseMatchPredicate ("$gte", v) = Right $ GTEMatchExpr v
parseMatchPredicate ("$ne", v) = Right $ NotMatchPred $ EqMatchExpr v

parseMatchPredicate ("$not", DocumentValue subexpr) =
    parseNotBody subexpr >>= (Right . NotMatchPred)

parseMatchPredicate ("$not", v) = Left Error {
    errCode = FailedToParse,
    errReason = "$not requires object but found: " ++ show v }

-- XXX: $exists accepts an arbitrary value as its argument, and interprets whether it is
-- $exists:true or $exists:false by coercing its argument to a boolean. It should probably only
-- accept a boolean as its input.
parseMatchPredicate ("$exists", v) =
    if isValueTruthy v
    then Right ExistsMatchExpr
    else Right $ NotMatchPred ExistsMatchExpr

-- $elemMatch:{} is an $elemMatch object whose subexpression is an empty AND.
parseMatchPredicate ("$elemMatch", DocumentValue Document { getFields = [] }) =
    Right $ ElemMatchObjectExpr $ AndMatchExpr []

-- XXX: $elemMatch object and $elemMatch value are very much different things, but they are spelled
-- the same way in the user-facing language. This requires parsers to do some awkward
-- disambiguation.
parseMatchPredicate ("$elemMatch", DocumentValue doc) =
    let hd = head (getFields doc) in
        -- Decide whether this is $elemMatch object or $elemMatch value based on whether we can
        -- parse the first field of the subobject as a MatchPredicate.
        case parseMatchPredicate hd of
            Left _ -> matchExprFromDocument doc >>= (Right . ElemMatchObjectExpr)
            Right _ -> mapM parseMatchPredicate (getFields doc)
                >>= (Right . ElemMatchValueExpr . AndMatchPred)

parseMatchPredicate ("$elemMatch", v) = Left Error {
    errCode = FailedToParse,
    errReason = "Expected nested Document inside $elemMatch but found: " ++ show v }

parseMatchPredicate (p, _) = Left Error {
    errCode = FailedToParse,
    errReason = "Unknown path-accepting match expression operator: " ++ p }

-- XXX: Path components in the matcher always explicitly traverse arrays. Users should be able to
-- indicate whether they want their path component to traverse arrays or not.
parsePathComponent :: String -> PathComponent
parsePathComponent ['0'] = PathComponent (FieldNameOrArrayIndex 0) ImplicitlyTraverseArrays
parsePathComponent (firstChar:rest) =
    if isDigit firstChar && firstChar /= '0' && all isDigit rest
    -- XXX: The FieldNameOrArrayIndex path component probably shouldn't exist in the first place,
    -- but the rules for when a path component is of this type are also surprising. A path component
    -- string might act as an array index if it is formatted as [1-9][0-9]*. Strings like "00",
    -- "01", "+1", and "1.0" do not act as array indexes.
    then PathComponent (FieldNameOrArrayIndex $ read (firstChar:rest)) ImplicitlyTraverseArrays
    else PathComponent (FieldName (firstChar:rest)) ImplicitlyTraverseArrays

-- Parses a string representation of a path, e.g. "a.b.c" to our internal Path representation.
parsePath :: String -> Path
parsePath str = map parsePathComponent (splitOn "." str)

-- Helper for parsing logical match expressions that accept an array of sub-expressions such as
-- $and, $or, and $nor.
parseLogicalExpr :: (String, Value) -> Either Error MatchExpr
parseLogicalExpr (op, ArrayValue Array { getElements = [] }) = Left Error {
    errCode = FailedToParse,
    errReason = "$and/$or/$nor must be a nonempty array" }

parseLogicalExpr (op, ArrayValue Array { getElements = elts }) =
    do
        subExprs <- mapM (\case
            (DocumentValue d) -> matchExprFromDocument d
            val -> Left Error {
                errCode = FailedToParse,
                errReason = "Found non-document inside $and/$or/$nor: " ++ show val }) elts
        case op of
            "$and" -> Right $ AndMatchExpr subExprs
            "$nor" -> Right $ NotMatchExpr $ OrMatchExpr subExprs
            "$or" -> Right $ OrMatchExpr subExprs

parseLogicalExpr (op, val) = Left Error {
    errCode = FailedToParse,
    errReason = "$and/$or/$nor must accept an array, but found: " ++ show val }

-- Special handling for {<path>: {$not: <match predicate>}}. The semantics of $not act to negate the
-- path match expression as a whole, rather than meaning "apply the negative of the match predicate
-- to each element selected by the path".
--
-- XXX: These semantics of $not are fine, but the syntax makes it confusing to users. Users should
-- probably position the $not on the outside, to make the meaning of the match expression more
-- clear. That is, {a: {$not: {$gt: 3}}} should probably be written as {$not: {a: {$gt: 3}}}.
liftNotPred :: Path -> [MatchPredicate] -> MatchExpr
liftNotPred path [NotMatchPred subPred] = NotMatchExpr $ PathAcceptingExpr path subPred
liftNotPred path [pred] = PathAcceptingExpr path pred

liftNotPred path (NotMatchPred subPred : tail) =
    AndMatchExpr $ NotMatchExpr (PathAcceptingExpr path subPred) : [liftNotPred path tail]

liftNotPred path (hd:tail) =
    AndMatchExpr $ PathAcceptingExpr path hd : [liftNotPred path tail]

-- Parse one (field, value) pair of a match expression document to a MatchExpr.
--
-- The semantics of the language is that there is an implicit AND. That is, the match expression
-- {<path1>: <pred1>, <path2>: <pred2>} is an implicit AND of pred1 and pred2.
parseOneExpr :: (String, Value) -> Either Error MatchExpr
-- XXX: I don't see any reason for {$and: []} to be illegal.
parseOneExpr ("$and", val) = parseLogicalExpr ("$and", val)
parseOneExpr ("$nor", val) = parseLogicalExpr ("$nor", val)
parseOneExpr ("$or", val) = parseLogicalExpr ("$or", val)

parseOneExpr ('$':keyword, v) = Left Error {
        errCode = FailedToParse,
        errReason = "Unknown top-level match expr: " ++ "$" ++ keyword }

-- XXX: We distinguish between implicit equality and all other path-accepting operators based on
-- inspection of the first (fieldname, value) pair in the subdocument. This is subject to injection.
parseOneExpr (path, DocumentValue Document { getFields = ('$':keyword, val):rest }) =
    let subexprs = ('$':keyword, val):rest in do
        preds <- mapM parseMatchPredicate subexprs
        Right $ liftNotPred (parsePath path) preds

-- The implicit equality case. For instance, {a: 9} is implicitly a match expression for whether "a"
-- is equal to 9.
parseOneExpr (path, value) =
    Right $ PathAcceptingExpr (parsePath path) (EqMatchExpr value)

-- Parses a Document to a MatchExpr.
matchExprFromDocument :: Document -> Either Error MatchExpr
matchExprFromDocument Document { getFields = [oneField] } = parseOneExpr oneField
matchExprFromDocument Document { getFields = fields } =
    mapM parseOneExpr fields >>= (Right . AndMatchExpr)

-- Parses a Value to a match expression. It is important that the data model admits representation
-- of queries themselves. This is necessary so that queries can be represented as data (e.g. for
-- serializing them in RPC messages, or for the server's system.profile collection).
matchExprFromValue :: Value -> Either Error MatchExpr
matchExprFromValue (DocumentValue d) = matchExprFromDocument d
matchExprFromValue val = Left Error {
    errCode = FailedToParse,
    errReason = "A MatchExpr must be specified as a Document, but found: " ++ show val }

-- Parses an extended JSON string to a match expression.
parseMatchExprString :: String -> Either Error MatchExpr
parseMatchExprString str =
    valueFromString str >>= matchExprFromValue

-- Parses a JSON value to a match expression.
parseMatchExprJson :: JSON.JSValue -> Either Error MatchExpr
parseMatchExprJson match =
    valueFromTextJson match >>= matchExprFromValue

-- Given a match expression and value represented as extended JSON strings, returns whether the
-- value matches (or an error if the match expression is invalid or cannot be applied).
evalStringMatchExpr :: String -> String -> Either Error Bool
evalStringMatchExpr expr value =
    do
        parsedExpr <- parseMatchExprString expr
        parsedVal <- valueFromString value
        evalMatchExpr parsedExpr parsedVal

-- Given a match expression and value represented as JSON, returns whether the value matches (or an
-- error if the match expression is invalid or cannot be applied).
evalJsonMatchExpr :: JSON.JSValue -> JSON.JSValue -> Either Error Bool
evalJsonMatchExpr expr value =
    do
        parsedExpr <- parseMatchExprJson expr
        parsedVal <- valueFromTextJson value
        evalMatchExpr parsedExpr parsedVal
