"""
Setup local mock server for tests
"""

import Builder

import os
import sys
import subprocess
import atexit


class MockServerSetup(Builder.Action):
    """
    Set up this machine for running the mock server test

    This action should be run in the 'pre_build_steps' or 'build_steps' stage.
    """

    def run(self, env):
        self.env = env
        python_path = env.config['variables'].get('python')
        # install dependency for mock server
        result = self.env.shell.exec(python_path,
                                     '-m', 'pip', 'install', 'h11', 'trio')
        import_result = self.env.shell.exec(python_path,
                                            '-c', 'import trio, h11')
        if result.returncode != 0 or import_result.returncode != 0:
            print(
                "Mock server failed to setup, skip the mock server tests.", file=sys.stderr)
            return

        # set cmake flag so mock server tests are enabled
        env.project.config['cmake_args'].append(
            '-DENABLE_MOCK_SERVER_TESTS=ON')

        base_dir = os.path.dirname(os.path.realpath(__file__))
        dir = os.path.join(base_dir, "..", "..", "tests", "mock_s3_server")
        os.chdir(dir)

        p = subprocess.Popen([python_path, "mock_s3_server.py"])

        @atexit.register
        def close_mock_server():
            p.terminate()
