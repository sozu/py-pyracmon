Installation
============

Prerequisite
------------

Pyracmon requires python 3.6 or higher.

Static typing functionalities are highly affected by python version.
Because of frequent update of python `typing` package, syntax assumed in this library has already got deprecated.
Those functionalities will be left not completely conforming to specifications of the package while they are unstable.

Currently supported DBMS are PostgreSQL (>= 10.0) and MySQL (>= 8.0).

Although pyracmon does not require any libraries for use by itself, it needs DB driver which conforms to DB-API 2.0.
`psycopg2 <https://pypi.org/project/psycopg2/>`_ (for PostgreSQL) and `PyMySQL <https://pypi.org/project/PyMySQL/>`_ are used in development.
Use them or some other library and tell it to pyracmon via `pyracmon.declare_models` .

Installation
------------

Pyracmon can be installed by using pip. ::

    pip install pyracmon==1.0.dev9