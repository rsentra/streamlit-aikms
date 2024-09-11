FROM python:3-slim

# WORKDIR /app

# RUN apt-get update && apt-get install -y \
#     build-essential \
#     curl \
#     software-properties-common \
#     git \
#     && rm -rf /var/lib/apt/lists/*

# RUN git clone https://github.com/streamlit/streamlit-example.git .
# 작업디렉토리를 app으로, c:\\temp와 같이 폴더 지정할 수도
WORKDIR /app  
# 파일을 복사
COPY . .      
#---------------
RUN pip3 install -r requirements.txt
#컨테이너가 수신할 포트
EXPOSE 8503
#컨테이너 헬스 체크
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
#실행될 컨테이너 구성
ENTRYPOINT ["streamlit", "run", "Main.py", "--server.port=8503", "--server.address=0.0.0.0"]