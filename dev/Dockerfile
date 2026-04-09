FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} dev && \
    useradd -u ${UID} -g dev -s /bin/bash -m dev

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        task-japanese \
        locales \
        git \
        gcc \
        libpq-dev \
        python3-dev \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* \
    && echo "ja_JP UTF-8" >> /etc/locale.gen \
    && locale-gen

ENV LANG ja_JP.UTF-8
ENV PYTHONUNBUFFERED 1

WORKDIR /home/dev
RUN chown -R dev:dev /home/dev

USER dev

RUN --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=.python-version,target=.python-version \
    uv sync --frozen --no-install-project

EXPOSE 5000
CMD ["sleep", "infinity"]