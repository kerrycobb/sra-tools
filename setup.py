from setuptools import setup, find_packages

setup(
    name="sratools",    
    version="0.1.0",
    packages=find_packages(), 
    package_data=dict(sratools=['data/asperaweb_id_dsa.openssh'])
)