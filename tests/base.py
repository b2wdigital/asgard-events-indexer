import os

from asynctest import TestCase
from asynctest.mock import CoroutineMock


class BaseTestCase(TestCase):
    maxDiff = None
    use_default_loop = True


LOGGER_MOCK = CoroutineMock(
    info=CoroutineMock(), debug=CoroutineMock(), exception=CoroutineMock()
)

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "fixtures"
)
