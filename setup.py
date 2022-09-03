from setuptools import find_packages, setup

setup(
    name='ls',
    version='0.0.1',
    description='最后的搜索',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'tornado',
        'pytest',
        'pytest-timeout',
        'coverage',
        'requests',
        'lxml',
        'cssselect',
        'requests-html'
    ],
    entry_points="""
    [console_scripts]
    kuaiso-web = ls.main:main
    kuaiso-db = ls.db:main
    kuaiso-spider = ls.spider:main
    """
)
