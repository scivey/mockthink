from setuptools import setup

version_path = 'mockthink/version.py'

exec(open(version_path).read())

setup(
    name="mockthink",
    zip_safe=True,
    version=VERSION,
    description="A pure-python in-memory mock of rethinkdb",
    url="http://github.com/scivey/mockthink",
    maintainer="Scott Ivey",
    maintainer_email="scott.ivey@gmail.com",
    packages=['mockthink'],
    package_dir={'mockthink': 'mockthink'},
    install_requires=['rethinkdb>=2.2.0,<2.3.0', 'dateutils', 'future']
)
