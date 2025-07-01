# TODO: Support ascend npu: ascendai/cann:ubuntu-python3.10-cann8.0.rc3.beta1
FROM python:3.10-slim

# 设置 pip镜像源为清华源
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_TIMEOUT=600

# ENV TZ=Asia/Shanghai

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# 更新包列表并安装 build-essential，它包含了 g++ 编译器和其它编译工具
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Install the dependencies
# 安装其它依赖并强制覆盖使用 headless
RUN pip install --no-cache-dir -r requirements.txt 

# TODO: Fix the requirements.txt file
## The diagnose module use a different python version thus the listed enviroment config is incorrect.
## The below line is just for the deployment test in March.
# COPY database/user.db /app/database/user.db
# Copy the rest of the application code into the container
COPY . .

# Expose the port that the app runs on
EXPOSE 1022

# Define the command to run the application
CMD ["gunicorn", "--bind", "0.0.0.0:1022", "app:app"]