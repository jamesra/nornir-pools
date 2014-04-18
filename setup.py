'''
Created on Aug 30, 2013

@author: u0490822
'''

from ez_setup import use_setuptools




if __name__ == '__main__':
    use_setuptools()

    from setuptools import setup

    install_requires = ["six"]

    extras_require = {"pp" : ["pp"]}

    tests_require = ["nose"]

    classifiers = ['Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 2.7']

    setup(name='nornir_pools',
          classifiers=classifiers,
          version='1.1.8',
          description="A helper library that wraps python threads, multiprocessing, a process pool for shell commands, and parallel python with the same interface",
          author="James Anderson",
          author_email="James.R.Anderson@utah.edu",
          url="https://github.com/nornir/nornir-pools",
          install_requires=install_requires,
          tests_require=tests_require,
          extras_require=extras_require,
          packages=["nornir_pools"], test_suite='test')
