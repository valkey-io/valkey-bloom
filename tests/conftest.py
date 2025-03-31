import subprocess
import pytest
import sys
import os

# Set the path to find and use the valkey-test-framework
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build/valkeytestframework')))

@pytest.fixture(params=['random-seed', 'fixed-seed'])
def bloom_config_parameterization(request):
    return request.param
