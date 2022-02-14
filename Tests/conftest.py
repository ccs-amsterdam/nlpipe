import pytest

import nlpipe.server


@pytest.fixture()
def app():
    return nlpipe.server.app
