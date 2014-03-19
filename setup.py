'''
Created on Aug 30, 2013

@author: u0490822
'''


from ez_setup import use_setuptools
from setuptools import setup, find_packages


if __name__ == '__main__':

    use_setuptools()

    install_requires = ["pp"]

    tests_require = ["pp", "nose"]

    setup(name='nornir_pools',
          version='1.1.6',
          description="A helper library that wraps python threads, multiprocessing, a process pool for shell commands, and parallel python with the same interface",
          author="James Anderson",
          author_email="James.R.Anderson@utah.edu",
          url="https://github.com/nornir/nornir-pools",
          install_requires=install_requires,
          tests_require=tests_require,
          packages=["nornir_pools"], test_suite='test')
