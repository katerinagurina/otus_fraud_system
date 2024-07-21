
import os
from setuptools import setup, find_packages
from distutils.core import setup, Extension

ext_modules = [Extension(name=f'{m}.*',
                         sources=[f'src/{m.replace(".", "/")}/*.py'])
               for m in find_packages(where='ml')]

setup(
    name='ml_block_otus_fraud',
    version=os.environ.get('VERSION', '0.0.0'),
    package_dir={'': 'ml'},
    include_package_data=True,
    packages=find_packages(where='ml'),
    ext_modules=ext_modules,
    python_requires='>=3.7',
    dependency_links=[
    ]
)
