"""Unit tests for importer (using pytest)."""

from os import path
import unittest

from beancount.ingest import regression_pytest as regtest
import run


IMPORTER = run.Importer()

print(path.dirname(__file__))


@regtest.with_importer(IMPORTER)
@regtest.with_testdir(path.dirname(__file__) + '/testfiles')
class TestImporter(regtest.ImporterTestBase):
    pass


if __name__ == '__main__':
    unittest.main()
