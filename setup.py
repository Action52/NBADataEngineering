from setuptools import setup

setup(
    name="dec",
    version="1.0",
    author="alfredo.leon",
    description="This is the Capstone project for Udacity's Data Engineering Nanodegree course.",
    install_requires=[
        "pandas",
        "jupyter",
        "psycopg2-binary",
        "tqdm",
        "apache-airflow==1.10.3",
        "boto3",
        "awscli",
        "WTForms==2.3.3",
        "marshmallow==2.21.0",
        "nbconvert"
    ]
)