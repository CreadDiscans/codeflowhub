from setuptools import setup, find_packages

setup(
    name='codeflowhub',
    version='0.0.1',
    description='workflow development tools',
    author='creaddiscans',
    author_email='creaddiscans@gmail.com',
    packages=find_packages(exclude=['template']),
    include_package_data=True,
    install_requires=[
    ],
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)