from setuptools import setup, Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True


setup(
    name="pyrecordio",
    version="0.0.5",
    description="recordio file format support",
    url="https://github.com/wangkuiyi/recordio",
    license="Apache 2.0",
    packages=["recordio"],
    package_data={"recordio": ["librecordio.so"]},
    distclass=BinaryDistribution,
)
