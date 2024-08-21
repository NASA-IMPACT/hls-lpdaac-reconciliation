from setuptools import find_packages, setup  # type: ignore

aws_cdk_extras = [
    "aws-cdk-lib==2.153.0",
    "constructs>=10.0.0,<11.0.0",
]

install_requires: list[str] = []

extras_require_test = [
    *aws_cdk_extras,
    "flake8~=7.0",
    "black~=24.1",
    "boto3~=1.34",
    "moto[s3,sqs]~=4.0",
    "pytest-cov~=5.0",
    "pytest~=8.0",
]

extras_require_dev = [
    *extras_require_test,
    "aws_lambda_typing~=2.18",
    "boto3-stubs[iam,lambda,s3,sqs,ssm]~=1.34",
    "botocore-stubs~=1.34",
    "isort~=5.13",
    "mypy~=1.8",
    "nodeenv~=1.8",
    "pre-commit~=3.6",
    "pre-commit-hooks~=4.5",
    "pyright~=1.1",
]

extras_require = {
    "test": extras_require_test,
    "dev": extras_require_dev,
}

setup(
    name="hls-lpdaac-reconciliation",
    version="0.1.0",
    python_requires=">=3.9",
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
