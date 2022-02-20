# Install base image (host OS Ubuntu)
FROM ubuntu:20.04

## Install tzdata and provide localization settings to install curl
RUN echo "tzdata tzdata/Areas select Europe" > /tmp/preseed.txt; \
    echo "tzdata tzdata/Zones/Europe select Moscow" >> /tmp/preseed.txt; \
    debconf-set-selections /tmp/preseed.txt && \
    apt-get update && \
    apt-get install -y tzdata

# Install apt and other packages
RUN apt-get -y install sudo && \
    sudo apt-get -y install cmake && \
    sudo apt-get -y install g++ gcc binutils libx11-dev libxpm-dev libxft-dev libxext-dev python3.10 python3-pip libssl-dev && \
    sudo apt-get -y install wget mc curl git

#Change Workdir into installation directory for ROOT_CERN
WORKDIR /usr/local/ROOT_CERN

#Download ROOT from CERN git-repo into /usr/local/ROOT_CERN/root_src
RUN git clone --branch latest-stable https://github.com/root-project/root.git root_src
RUN ls

#Install and build ROOT_CERN
RUN mkdir root_build root_install
WORKDIR /usr/local/ROOT_CERN/root_build
RUN cmake -DPYTHON_EXECUTABLE=/usr/bin/python3.8 -DPYTHON_INCLUDE_DIR=/usr/include/python3.8/ -DPYTHON_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython3.8.so python=ON and python3=ON -Dbuiltin_fftw3=ON -Droofit=ON -Dtmva=ON ../root_src
RUN cmake -DCMAKE_INSTALL_PREFIX=../root_install ../root_src
RUN cmake --build . -- install -j2

#Source ROOT_CERN and make env variables
RUN /bin/bash -c "source ../root_install/bin/thisroot.sh"
ENV ROOTSYS=/usr/local/ROOT_CERN/root_install
ENV PATH=$ROOTSYS/bin:$PYTHONDIR/bin:$PATH
ENV LD_LIBRARY_PATH=$ROOTSYS/lib:$PYTHONDIR/lib:$LD_LIBRARY_PATH
ENV PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH

#Make directory for Binance Writer project
WORKDIR /usr/local/BinanceWriter

#Copy requirements file into project directory
COPY requirements.txt .

#Install required dependencies
RUN pip install -r requirements.txt

#Copy scr project files
COPY src/ .