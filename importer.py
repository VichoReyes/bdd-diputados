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


# TODO diagrama
crear_comuna = '''
CREATE TABLE IF NOT EXISTS comuna (
    distrito int references distrito(numero),
    nombre varchar(50),
    numero int primary key -- actualizar E-R
);
'''

crear_distrito = '''
CREATE TABLE IF NOT EXISTS distrito (
    numero int primary key
);
'''

# TODO diagrama
crear_diputado = '''
CREATE TABLE IF NOT EXISTS diputado (
    id int primary key,
    nombre varchar(50),
    a_paterno varchar(50), -- actualizar E-R
    a_materno varchar(50), -- actualizar E-R
    nacimiento date, -- actualizar E-R
    distrito int references distrito(numero)
);
'''

crear_p_ley = '''
CREATE TABLE IF NOT EXISTS p_ley (
    foreign_id int,
    boletin varchar(10) primary key,
    resumen varchar(200), -- actualizar E-R
    fecha_ingreso date
);
'''

crear_voto = '''
CREATE TABLE IF NOT EXISTS voto (
    diputado int references diputado(id),
    p_ley varchar(10) references p_ley(boletin),
    opcion varchar(20)
);
'''

insertar_diputado = '''
INSERT INTO diputado
(id, nombre, a_paterno, a_materno, nacimiento, distrito)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
'''

insertar_comuna = '''
INSERT INTO comuna (distrito, nombre, numero) VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING;
'''


insertar_distrito = '''
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
        exec_sql(insertar_comuna, (num_dist, nombre, numero))


def crear_tablas():
    exec_sql(crear_distrito)
    exec_sql(crear_comuna)
    exec_sql(crear_diputado)
    exec_sql(crear_p_ley)
    exec_sql(crear_voto)


def diputados():
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSDiputado.asmx/retornarDiputadosXPeriodo?prmPeriodoId=8"
    content = get_with_cache("diputados.xml", url)
    for diputado_periodo in content:
        diputado = diputado_periodo[2]
        assert clean_tag(diputado[0]) == "Id"
        assert clean_tag(diputado[1]) == "Nombre"
        assert clean_tag(diputado[3]) == "ApellidoPaterno"
        assert clean_tag(diputado[4]) == "ApellidoMaterno"
        tiene_nacimiento = clean_tag(diputado[5]) == "FechaNacimiento"
        assert clean_tag(diputado_periodo[3]) == "Distrito"
        dipid = int(diputado[0].text)
        nombre = diputado[1].text
        a_pat = diputado[3].text
        a_mat = diputado[4].text
        nacimiento = diputado[5].text.split(
            'T')[0] if tiene_nacimiento else None
        num_dist = int(diputado_periodo[3][0].text)
        exec_sql(insertar_diputado, vals=(dipid, nombre,
                                          a_pat, a_mat, nacimiento, num_dist))


def distritos():
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSComun.asmx/retornarDistritos"
    content = get_with_cache("distritos.xml", url)
    for distrito in content:
        assert clean_tag(distrito[0]) == "Numero"
        assert clean_tag(distrito[1]) == "Comunas"
        num_dist = int(distrito[0].text)
        exec_sql(insertar_distrito, vals=(num_dist,))
        crear_comunas(distrito[1], num_dist)


tareas = ["crear_tablas", "distritos", "diputados"]
if __name__ == "__main__":
    for t in tareas:
        # O(n^2) pero a quién le importa
        if t in sys.argv:
            exec(t+"()")
