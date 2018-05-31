"""The unittest.TestCase for MQL Haskell tests."""

from __future__ import absolute_import

from . import interface
from ... import core
from ... import utils


class MqlHaskellMOdelTestCase(interface.ProcessTestCase):
    """A MQL Haskell Model test to execute."""

    REGISTERED_NAME = "mql_haskell_model_test"

    def __init__(self, logger, program_executable, program_options=None):
        """Initialize the MqlHaskellMOdelTestCase with the executable to run."""

        interface.ProcessTestCase.__init__(self, logger, "MQL Haskell Model test", program_executable)

        self.logger.info("MqlHaskellMOdelTestCase::__init__ program_executable=%s", program_executable)

        self.program_executable = "mql-model/dist/build/mql/mql"
        self.program_options = utils.default_if_none(program_options, {}).copy()


    def _make_process(self):
        return core.process.Process(self.logger, [self.program_executable, "--test", "mql-model/test/tests.json", "--prefix", "mql-model/"], **self.program_options)
