FROM --platform=linux/amd64 python:3.12-slim-bullseye as build
LABEL Maintener="Fernando.Bytecode"

ENV LOOKERSDK_BASE_URL=''
ENV LOOKERSDK_VERIFY_SSL=true	
ENV LOOKERSDK_TIMEOUT=120	
ENV LOOKERSDK_CLIENT_ID=''
ENV LOOKERSDK_CLIENT_SECRET=''
ENV SERVICE_ACCOUNT=''
ENV SENDGRID_API_KEY=''


WORKDIR /usr/app/src
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT [ "python","-u","main.py" ]

CMD ["--help"] 