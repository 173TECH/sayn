from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

long_description = 'This is my long description.'

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
        version ='1.0.0',
        author ='Robin Watteaux',
        author_email ='robin@173tech.com',
        url ='',
        description ='SAYN test.',
        long_description = long_description,
        long_description_content_type ="text/markdown",
        license ='173',
        packages = find_packages(),
        package_data=sayn_package_data,
        include_package_data=True,
        entry_points ={
            'console_scripts': sayn_commands_list
        },
        classifiers =(
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: 173 License",
            "Operating System :: OS Independent",
        ),
        keywords ='python SAYN',
        install_requires = requirements,
        zip_safe = False
)
