import pytest

@pytest.fixture(params=['random-seed', 'fixed-seed'])
def bloom_config_parameterization(request):
    return request.param