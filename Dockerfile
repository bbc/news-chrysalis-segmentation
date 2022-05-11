FROM jupyter/scipy-notebook:4cdbc9cdb7d1

# # # # # # # # # # # # # # #
# install system dependencies
# # # # # # # # # # # # # # #
USER root
# example
#RUN apt-get update && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt

# # # # # # # # # # # # # # #
# install python dependencies
# # # # # # # # # # # # # # #
USER jovyan
COPY requirements.txt /usr/local/bin/requirements.txt
WORKDIR /usr/local/bin/
RUN pip install -r requirements.txt

# # # # # # # # # # # # # # #
# install any other dependencies (conda, download some resources etc)
# # # # # # # # # # # # # # #
# example
#RUN python -m spacy download en_core_web_lg && python -m spacy download en_core_web_sm
#RUN conda install --yes faiss-cpu -c pytorch && conda install --yes numba=0.43.1

# cd into folder where our stuff will be
WORKDIR /mnt/