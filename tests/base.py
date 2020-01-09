from asynctest import TestCase
from asynctest.mock import CoroutineMock


class BaseTestCase(TestCase):
    maxDiff = None
    use_default_loop = True


LOGGER_MOCK = CoroutineMock(
    info=CoroutineMock(), debug=CoroutineMock(), exception=CoroutineMock()
)
