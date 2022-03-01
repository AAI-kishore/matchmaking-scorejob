FROM python:3.7

#Create the working directory
RUN mkdir -p /usr/src/app

#Set the working directory
WORKDIR /usr/src/app

#copy all the data files
ADD . /usr/src/app/

#Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the Flask port
EXPOSE 9080

ENTRYPOINT [ "python" ]
CMD [ "server.py" ]
