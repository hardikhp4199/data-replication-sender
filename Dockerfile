#FROM gcr.io/infra-337510/artifacts/golang-alpine:1.19.2-alpine3.16

FROM golang:1.19.2-alpine3.16

# Create source and release directory 
RUN mkdir -p /usr/src/app &&  mkdir -p /usr/local/bin/app/cert  

# Set working directory to ./app
WORKDIR /usr/src/app

# Copy source code to source directory
COPY . /usr/src/app

# Copy certificate to release directory  [ temporary purpose]
#COPY ./cert /usr/local/bin/app/cert

# Install git package for downloading go modules
#RUN apk add git 

# Install packages for confluent kafka client
RUN apk add gcc libc-dev librdkafka-dev pkgconf 

# To Download our go programs dependencies 
RUN go mod download 

# Build go application (this command will generate executable file)
RUN go build -tags musl -o datareplicationsender

# Remove git package used for downloading go modules
#RUN apk del git 

# Remove packages used for confluent kafka client
RUN apk del gcc libc-dev librdkafka-dev pkgconf 

# Move executable file to release directory 
RUN  mv ./datareplicationsender /usr/local/bin/app

# Move Config Folder to release directory 
RUN  mv ./config /usr/local/bin/app

# Remove config.go file from config folder to /usr/local/bin/app path
RUN rm -rf /usr/local/bin/app/config/config.go

# Remove source directory 
RUN rm -fr /usr/src/app

# Set working directory to release
WORKDIR /usr/local/bin/app

# This Command will execute the executable file
CMD ["./datareplicationsender"]