from setuptools import setup, find_packages
from marianatek import __version__

setup(name='marianatek',
      description='&KO is going big and extracting data from -- MARIANA TEK',
      author='Kyle Ogilvie',
      author_email='kyle@kyleogilve.com',
      url='https://www.kyleogilvie.com/',
      packages=find_packages(),
      version=__version__,
      install_requires=[
        "certifi==2021.10.8",
        "charset-normalizer==2.0.11",
        "idna==3.3",
        "requests==2.27.1",
        "urllib3==1.26.8",
      ]
)
