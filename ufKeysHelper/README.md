# UfKeysHelper

TBv2 can encrypt user feedback, for those programs where the UF might be sensitive, or people might be nervous
about leaving their real thoughts. The actual UF is encrypted with AWS, and the AWS key is encrypted with the
public key of an RSA key pair. This function will retrieve the public portion of that, so that the ACM can add
it to a deployment when desired. If there is not already a key pair for a given program and deployment, a new
key pair will be created and persisted. 

It is important that the private key stay private _(duh!)_.


___

This function uses the Python "cryptography" package, which has binary components, and is not
provided by default in the Lambda environment. We provide it as a "layer", based on information found in the
following web pages.

[Lambda Not Loading Cryptography Shared Library](https://stackoverflow.com/questions/55508626/lambda-not-loading-cryptography-shared-library)   
[How to Install Python 3.9 on Amazon Linux 2](https://tecadmin.net/install-python-3-9-on-amazon-linux/)<br>
[Docker: Copying files from Docker container to host](https://stackoverflow.com/questions/22049212/docker-copying-files-from-docker-container-to-host)<br>
[How to Install Python Packages for AWS Lambda Layers?](https://www.geeksforgeeks.org/how-to-install-python-packages-for-aws-lambda-layers/)<br>

### Spin up an instance of amazonlinux
This is the OS environment in which the Lambda function will run. Opens to a bash prompt
running as root.

    sudo docker run -it amazonlinux bash

I like to set up some conenience aliases:

    alias ll='ls -l'
    alias la='ls -a'

### Build Python 3.9
Amazonlinux is extremely bare-bones, so the first step is to install the tools needed to build and install
Python and to prepare the cryptography package.

    yum install gcc openssl-devel bzip2-devel libffi-devel wget tar gzip make zip -y

Now we're ready to build, install, and run Python.

    cd /opt 
    wget https://www.python.org/ftp/python/3.9.16/Python-3.9.16.tgz
    tar xzf Python-3.9.16.tgz
    cd Python-3.9.16 
    ./configure --enable-optimizations 
    make altinstall
    rm -f /opt/Python-3.9.16.tgz
    python3.9 -V
    mkdir -p /home/dev # or whatever directory you prefer 
    cd /home/dev 
    python3.9 -m venv python
    source ./python/bin/activate

### Install cryptography
With Python up and running, this part is trivial. From the `/home/dev` directory:

    pip install crytography

### Copy the files out of Docker
The contents of the virtual env (`/home/dev/python`) will look like this:

    |-- bin
    |   |-- . . .
    |-- include
    |-- lib
    |   `-- python3.9
    |       `-- site-packages
    |           |-- _cffi_backend.cpython-39-x86_64-linux-gnu.so
    |           |-- _distutils_hack
    |           |-- cffi
    |           |-- cffi-1.15.1.dist-info
    |           |-- cryptography
    |           |-- cryptography-39.0.1.dist-info
    |           |-- distutils-precedence.pth
    |           |-- pip
    |           |-- pip-23.0.dist-info
    |           |-- pkg_resources
    |           |-- pycparser
    |           |-- pycparser-2.21.dist-info
    |           |-- setuptools
    |           `-- setuptools-58.1.0.dist-info
    |-- lib.zip
    |-- lib64 -> lib
    `-- pyvenv.cfg

Packages `cffi`, `cryptography`, and `pycparser` are installed by the `pip install cryptography` command, so zip all of 
those together:

    zip -r cryptography.zip python/lib/python3.9/site-packages/*cffi* python/lib/python3.9/site-packages/crypto* python/lib/python3.9/site-packages/pycp*
 
From the host OS, copy the file out of Docker. First find the running instance container id:

    % docker ps
    CONTAINER ID   IMAGE         COMMAND   CREATED       STATUS       PORTS     NAMES
    bff96da97d13   amazonlinux   "bash"    3 hours ago   Up 3 hours             silly_wing
 
Here the container id is `silly_wing`, but it will be different every time. Now copy the file

    % docker cp silly_wing:/home/dev/cryptography.zip .

Upload the `cryptography.zip` file as an AWS Lambda layer.
