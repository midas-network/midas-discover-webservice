import pytest

from project import create_app
from tests.unit.test_queries import test_author_pull

@pytest.fixture(scope='module')
def test_client():
    # Set the Testing configuration prior to creating the Flask application
    flask_app = create_app()

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            yield testing_client  # this is where the testing happens!

@pytest.fixture(scope='function')
def log_in_default_user(test_client):
    test_client.get('/authors/overlap/',
                     data=test_author_pull.query_params)

    print('hello world')

    yield  # this is where the testing happens!
    print('bye world')


@pytest.fixture(scope='module')
def cli_test_client():
    flask_app = create_app()

    runner = flask_app.test_cli_runner()

    yield runner

    