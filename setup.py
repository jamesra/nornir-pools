'''
Created on Aug 30, 2013

@author: u0490822
'''

from ez_setup import use_setuptools

if __name__ == '__main__':
    use_setuptools()

    from setuptools import setup, find_packages

    install_requires = ["six"]

    packages = find_packages()

    # extras_require = {"pp" : ["pp"]}
    extras_require = {}

    tests_require = ["nose"]

    classifiers = ['Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 2.7']

    setup(name='nornir_pools',
          zip_safe=True,
          classifiers=classifiers,
          version='1.3.3',
          description="A helper library that wraps python threads, multiprocessing, a process pool for shell commands, and parallel python with the same interface",
          author="James Anderson",
          author_email="James.R.Anderson@utah.edu",
          url="https://github.com/nornir/nornir-pools",
          install_requires=install_requires,
          tests_require=tests_require,
          extras_require=extras_require,
          packages=packages, test_suite='test')
