FROM continuumio/miniconda3

COPY environment.yml /tmp/environment.yml

RUN pip install --upgrade pip

RUN conda clean -all
RUN conda clean --index-cache
RUN conda update conda

RUN conda install -c conda-forge pyscipopt

RUN conda env create -f /tmp/environment.yml && \
    conda clean -afy && \
    rm /tmp/environment.yml

# Активируем Conda-окружение
SHELL ["conda", "run", "-n", "matcher", "/bin/bash", "-c"]

# Копируем проект в контейнер
COPY . /matcher

# Устанавливаем рабочую директорию
WORKDIR /matcher

# Указываем команду для запуска вашего проекта
CMD ["python", "-m", "matcher"]