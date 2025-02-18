FROM ubuntu:22.04

# update and install
RUN apt update && \
    apt install -y python3 python3-pip vim ssh

# install python packages
RUN pip3 install psycopg2-binary \
                 kubernetes \
                 pandas==2.2 \
                 pyarrow==17 \
                 python-dotenv


# set timezone
ENV TZ=Europe/Stockholm
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# add scripts last
COPY cpp_master_v2.py /cpp/
