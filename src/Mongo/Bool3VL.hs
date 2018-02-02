module Mongo.Bool3VL (
    Bool3VL(..),

    convertTo3VL,
    convertToBool,
    and3VL,
    or3VL,
    not3VL,
    ) where

-- Three valued logic to handle NULLs as expected by SQL
data Bool3VL = Unknown3VL | False3VL | True3VL deriving (Eq, Show)

convertTo3VL::Bool->Bool3VL
convertTo3VL True = True3VL
convertTo3VL False = False3VL

convertToBool::Bool3VL->Bool
convertToBool Unknown3VL = False
convertToBool False3VL = False
convertToBool True3VL = True

-- Kleene tables, see https://en.wikipedia.org/wiki/Three-valued_logic
and3VL::Bool3VL->Bool3VL->Bool3VL
and3VL Unknown3VL True3VL = Unknown3VL
and3VL True3VL Unknown3VL = Unknown3VL
and3VL Unknown3VL Unknown3VL = Unknown3VL
and3VL True3VL True3VL = True3VL
and3VL _ _ = False3VL

or3VL::Bool3VL->Bool3VL->Bool3VL
or3VL Unknown3VL False3VL = Unknown3VL
or3VL False3VL Unknown3VL = Unknown3VL
or3VL Unknown3VL Unknown3VL = Unknown3VL
or3VL False3VL False3VL = False3VL
or3VL _ _ = True3VL

not3VL::Bool3VL->Bool3VL
not3VL Unknown3VL = Unknown3VL
not3VL True3VL = False3VL
not3VL False3VL = True3VL
