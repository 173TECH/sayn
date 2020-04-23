from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

long_description = '''
    SAYN is data modelling and processing framework which enables analytics teams to build robust,
    scalable and efficient data infrastructures.
    It provides an easy way to orchestrate tasks, including SQL and Python, and to build data processes in minutes.
'''

sayn_commands_list = [
    'sayn = sayn.commands.sayn_powers:cli'
]

sayn_package_data = {
    'sayn': [
        'start_project/sayn_project_base/*',
        'start_project/sayn_project_base/.gitignore',
        'start_project/sayn_project_base/python/*',
        'start_project/sayn_project_base/sql/*',
        'start_project/sayn_project_base/sql/*/*',
        'start_project/sayn_project_base/logs/*',
    ]
}

setup(
        name ='SAYN',
        version ='0.1.0',
        author = ['Robin Watteaux', 'Adrian Macias'],
        author_email = ['robin@173tech.com', 'adrian@173tech.com'],
        url = 'https://173TECH.github.io/sayn',
        description = 'Data modelling and processing framework.',
        long_description = long_description,
        long_description_content_type ="text/markdown",
        license ='Apache 2.0',
        packages = find_packages(),
        package_data=sayn_package_data,
        include_package_data=True,
        entry_points ={
            'console_scripts': sayn_commands_list
        },
        classifiers =(
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: Apache 2.0",
            "Operating System :: OS Independent",
        ),
        keywords ='data modelling and processing framework',
        install_requires = requirements,
        zip_safe = False
)
