import Language.Haskell.HLint (hlint)
import System.Exit (exitFailure, exitSuccess)

-- Lint all .hs files in the root directory and all its subdirectories.
lintDirs = ["."]
ignoredPatterns = ["Use head"]
arguments = lintDirs ++ map ("--ignore=" ++) ignoredPatterns

main :: IO ()
main = do
    hints <- hlint arguments
    if null hints then exitSuccess else exitFailure
