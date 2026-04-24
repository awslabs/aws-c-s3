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

    def _find_python_with_pip(self):
        """
        Find a Python executable that has pip.
        The builder may run under a stripped system Python (e.g. Python 3.6 on Ubuntu 18)
        that has no pip or ensurepip.
        """
        import shutil

        # Candidates in preference order: newer Pythons first, then sys.executable
        candidates = ['python3.12', 'python3.11', 'python3.10', 'python3.9', 'python3.8', sys.executable]

        for candidate in candidates:
            python = shutil.which(candidate)
            if python is None:
                continue
            result = subprocess.run([python, '-m', 'pip', '--version'],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode == 0:
                print("Found Python with pip: {}".format(python))
                return python

        # No Python with pip found; return sys.executable and let pip install fail with a clear error
        return sys.executable

    def run(self, env):
        if not env.project.needs_tests(env):
            print("Skipping mock server setup because tests disabled for project")
            return

        self.env = env
        python_path = self._find_python_with_pip()

        # install dependency for mock server
        self.env.shell.exec(python_path,
                            '-m', 'pip', 'install', 'h11', 'trio', 'proxy.py', check=True)
        # check the deps can be import correctly
        self.env.shell.exec(python_path,
                            '-c', 'import h11, trio', check=True)

        # set cmake flag so mock server tests are enabled
        env.project.config['cmake_args'].extend(
            ['-DENABLE_MOCK_SERVER_TESTS=ON', '-DASSERT_LOCK_HELD=ON'])

        base_dir = os.path.dirname(os.path.realpath(__file__))
        dir = os.path.join(base_dir, "..", "..", "tests", "mock_s3_server")

        p1 = subprocess.Popen([python_path, "mock_s3_server.py"], cwd=dir)
        try:
            p2 = subprocess.Popen("proxy", cwd=dir)
        except Exception as e:
            # Okay for proxy to fail starting up as it may not be in the path
            print(e)
            p2 = None

        @atexit.register
        def close_mock_server():
            p1.terminate()
            if p2 != None:
                p2.terminate()
