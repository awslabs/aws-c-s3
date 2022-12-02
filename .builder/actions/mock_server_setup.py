"""
Setup local mock server for tests
"""

import Builder

import os
import subprocess


class MockServerSetup(Builder.Action):
    """
    Set up this machine for running the mock server test

    This action should be run in the 'pre_build_steps' or 'build_steps' stage.
    """

    def run(self, env):
        self.env = env

        # set cmake flag so mock server tests are enabled
        env.project.config['cmake_args'].append(
            '-DENABLE_MOCK_SERVER_TESTS=ON')

        if os.name == 'nt':
            python_execute = "python"
        else:
            python_execute = "python3"

        # install dependency for mock server
        print(self.env.shell.exec(python_execute, '-m', 'install', 'h11', 'trio'))
        base_dir = os.path.dirname(os.path.realpath(__file__))
        dir = os.path.join(base_dir, "..", "..", "tests", "mock_s3_server")
        os.chdir(dir)

        subprocess.Popen([python_execute, "mock_s3_server.py"])
