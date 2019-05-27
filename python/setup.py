from setuptools import setup, Extension, find_packages


setup(
    name="pyrecordio",
    version="0.0.6",
    description="recordio file format support",
    url="https://github.com/wangkuiyi/recordio",
    license="Apache 2.0",
    packages=find_packages(exclude=["tests"]),
    build_golang={'root': 'github.com/wangkuyi/recordio'},
    ext_modules=[Extension('recordio.librecordio', ['crecordio.go'])],
    setup_requires=['setuptools-golang', 'pytest-runner'],
    tests_require=['pytest'],
)
