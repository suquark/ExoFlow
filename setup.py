from setuptools import setup, find_packages

setup(
    name="exoflow",
    version="0.1.0",
    description="ExoFlow",
    author="suquark",
    author_email="your.email@example.com",
    url="https://github.com/suquark/ExoFlow",
    packages=find_packages(),
    install_requires=[
        "ray==2.0.1",
        "boto3==1.24.63",
        "smart-open==6.0.0",
        "tqdm==4.64.1",
        "uvicorn==0.16.0",
        "fastapi==0.79.1",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
