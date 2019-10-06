import requests
import psycopg2
import xml.etree.ElementTree as ET
import sys

conn = psycopg2.connect(database="bbdd", user="vicente", password="asdf")


def exec_sql(statement: str, vals=()) -> None:
    c = conn.cursor()
    c.execute(statement, vals)
    conn.commit()
    c.close()


def clean_tag(el: ET.Element) -> str:
    return el.tag.split('}')[-1]


crear_tabla_comuna = '''
CREATE TABLE IF NOT EXISTS comuna (
    distrito int references distrito(numero),
    nombre varchar(50),
    numero int primary key
);
'''

crear_tabla_distrito = '''
CREATE TABLE IF NOT EXISTS distrito (
    numero int primary key
);
'''

crear_comuna = '''
INSERT INTO comuna (distrito, nombre, numero) VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING;
'''


crear_distrito = '''
INSERT INTO distrito (numero) VALUES (%s)
ON CONFLICT DO NOTHING;
'''


def get_with_cache(filename: str, url: str) -> ET.Element:
    if filename:
        try:
            with open(filename) as src:
                return ET.parse(src).getroot()
        except FileNotFoundError:
            pass
    r = requests.get(url)
    assert r.status_code == 200
    if filename:
        with open(filename, "w") as dst:
            print(r.text, file=dst)
    return ET.fromstring(r.text)


def crear_comunas(comunas, num_dist):
    for com in comunas:
        assert clean_tag(com[0]) == "Numero"
        assert clean_tag(com[1]) == "Nombre"
        nombre = com[1].text
        numero = int(com[0].text)
        exec_sql(crear_comuna, (num_dist, nombre, numero))


def crear_tablas():
    exec_sql(crear_tabla_distrito)
    exec_sql(crear_tabla_comuna)


def distritos():
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSComun.asmx/retornarDistritos"
    content = get_with_cache("distritos.xml", url)
    for distrito in content:
        assert clean_tag(distrito[0]) == "Numero"
        assert clean_tag(distrito[1]) == "Comunas"
        num_dist = int(distrito[0].text)
        exec_sql(crear_distrito, vals=(num_dist,))
        crear_comunas(distrito[1], num_dist)


tareas = ["crear_tablas", "distritos"]
if __name__ == "__main__":
    for t in tareas:
        # O(n^2) pero a quién le importa
        if t in sys.argv:
            exec(t+"()")
