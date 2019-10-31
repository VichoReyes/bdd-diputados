# Cámara de Diputados Data Importer

This repo contains the script for automatically downloading data from the Cámara de Diputados, including which representatives voted for which laws, which provinces they represent, etc.

This is done using an XML-based API the government provides when it is possible, and some ugly hacks when it isn't.

## Usage

First, create a Postgres database called `bbdd` that can be accessed
with username `vicente` and password `asdf`,
or change the aproppiate line in `importer.py` (it's line 7 at the time of writing).

Then run

```bash
$ python importer.py <tasks>
```
**Warning:** The script will cache lots of files in the current working directory,
so don't run it in a directory you don't want polluted.

One or more tasks can be specified, and the available ones are:
- crear_tablas
- distritos
- diputados
- votos2019

The order in which you specify them doesn't matter
