from pathlib import Path
from setuptools import find_packages, setup  # type: ignore

aws_cdk_extras = [
    "aws-cdk-lib>=2",
    "constructs>=10.0.0",
]

install_requires: list[str] = []

extras_require_test = [
    *aws_cdk_extras,
    "flake8",
    "black",
    "boto3",
    "moto[s3,sns,sqs]",
    "pytest-cov",
    "pytest",
    "pytest-vcr",
    "vcrpy",
]

extras_require_dev = [
    *extras_require_test,
    "aws_lambda_typing",
    "boto3-stubs[iam,lambda,s3,sns,sqs]",
    "botocore-stubs",
    "isort",
    "mypy",
    "nodeenv",
    "pre-commit",
    "pre-commit-hooks",
    "pyright",
]

extras_require = {
    "test": extras_require_test,
    "dev": extras_require_dev,
}

setup(
    name="hls-lpdaac-reconciliation",
    version="0.1.0",
    python_requires=">=3.12",
    author="Development Seed",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        ".": [
            "cdk.json",
        ],
    },
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
)
