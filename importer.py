import requests
import psycopg2
import xml.etree.ElementTree as ET
import sys
import re

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
    resumen text, -- actualizar E-R
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

crear_materia = '''
CREATE TABLE IF NOT EXISTS materia (
    id int primary key,
    nombre text
);
'''

crear_trata_sobre = '''
CREATE TABLE IF NOT EXISTS trata_sobre (
    materia int references materia(id),
    boletin varchar(10) references p_ley(boletin)
);
'''

insertar_trata_sobre = '''
INSERT INTO trata_sobre
(materia, boletin)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
'''

insertar_materia = '''
INSERT INTO materia
(id, nombre)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
'''

insertar_p_ley = '''
INSERT INTO p_ley
(foreign_id, boletin, resumen, fecha_ingreso)
VALUES (%s, %s, %s, %s)
ON CONFLICT DO NOTHING;
'''

insertar_voto = '''
INSERT INTO voto
(diputado, p_ley, opcion)
VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING;
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
    exec_sql(crear_materia)
    exec_sql(crear_trata_sobre)


prefijo_horrible = "{http://opendata.camara.cl/camaradiputados/v1}"

def hijo(nodo: ET.Element, tag: str):
    ret = nodo.find(prefijo_horrible+tag)
    assert ret != None
    return ret


def votos2019():
    regex = re.compile("[0-9]+-[0-9]+")
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSLegislativo.asmx/retornarVotacionesXAnno?prmAnno=2019"
    content = get_with_cache("votos2019.xml", url)
    for vot in content:
        if vot.find(prefijo_horrible+"Tipo").text != "Proyecto de Ley":
            continue
        votid = int(hijo(vot, "Id").text)
        boletin = regex.search(hijo(vot, "Descripcion").text).group(0)
        insertar_p_si_falta(boletin)
        insertar_votacion(votid, boletin)


def existe_p_ley(boletin: str) -> bool:
    c = conn.cursor()
    c.execute("SELECT boletin FROM p_ley WHERE boletin = %s", (boletin,))
    return c.fetchone() is not None


def insertar_p_si_falta(boletin: str):
    if existe_p_ley(boletin):
        return
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSLegislativo.asmx/retornarProyectoLey?prmNumeroBoletin="+boletin
    content = get_with_cache("p_ley_"+boletin+".xml", url)
    # assert clean_tag(content[8]) == "Materias"
    forid = hijo(content, "Id").text
    resumen = hijo(content, "Nombre").text
    fecha = hijo(content, "FechaIngreso").text.split('T')[0]
    exec_sql(insertar_p_ley, (forid, boletin, resumen, fecha))
    materias = hijo(content, "Materias")
    for materia in materias:
        mat_id = int(hijo(materia, "Id").text)
        nombre = hijo(materia, "Nombre").text
        exec_sql(insertar_materia, (mat_id, nombre))
        exec_sql(insertar_trata_sobre, (mat_id, boletin))



def insertar_votacion(id: int, boletin: str):
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSLegislativo.asmx/retornarVotacionDetalle?prmVotacionId=" + \
        str(id)
    content = get_with_cache("votac"+str(id)+".xml", url)
    try:
        votos = content.find(prefijo_horrible+"Votos")
        assert votos
    except:
        print("Problema en la votación con id", id)
        return  # Saltarse el caso problemático
    for voto in votos:
        id_diputado = int(voto[0][0].text)
        opcion = voto[1].text
        try:
            exec_sql(insertar_voto, vals=(id_diputado, boletin, opcion))
        except psycopg2.errors.ForeignKeyViolation:
            conn.rollback()
            insertar_diputado_desde_id(id_diputado)
            exec_sql(insertar_voto, vals=(id_diputado, boletin, opcion))

distritos_actuales = [17,10,27,23,26,17,28,26,8,1,9,16,14,24,
             26,28,20,28,7,27,9,12,19,10,3,15,23,7,4,
             4,14,12,10,11,8,7,9,13,5,25,10,24,6,5,
             11,5,2,23,14,9,20,7,2,25,3,8,11,7,6,24,
             10,19,12,13,25,6,9,15,26,12,8,14,27,6,17,
             11,15,6,18,8,16,23,23,1,8,23,21,17,13,4,20
             ,18,4,21,5,3,25,9,9,20,12,6,22,22,21,12,21,
             3,17,11,22,18,17,1,7,20,24,20,19,10,23,5,19,
             4,26,14,19,15,6,16,17,8,14,15,13,20,10,7,2,
             16,11,18,7,21,12,20,3,5,22,6,8,24,5,10,13]

def diputados():
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSDiputado.asmx/retornarDiputadosXPeriodo?prmPeriodoId=9"
    content = get_with_cache("diputados.xml", url)
    assert len(content) == len(distritos_actuales)
    for num_dist, diputado_periodo in zip(distritos_actuales, content):
        diputado = hijo(diputado_periodo, "Diputado")
        insertar_diputado_particular(diputado, num_dist=num_dist)


def insertar_diputado_desde_id(dipu_id: int, num_dist=None):
    url = "http://opendata.camara.cl/camaradiputados/WServices/WSDiputado.asmx/retornarDiputado?prmDiputadoId=" + \
        str(dipu_id)
    diputado = get_with_cache('diputado_'+str(dipu_id)+'.xml', url)
    insertar_diputado_particular(diputado, num_dist=num_dist)


def insertar_diputado_particular(diputado: ET.Element, num_dist=None):
    dipid = int(hijo(diputado, "Id").text)
    nombre = hijo(diputado, "Nombre").text
    a_pat = hijo(diputado, "ApellidoPaterno").text
    a_mat = hijo(diputado, "ApellidoMaterno").text
    try:
        nacimiento = hijo(diputado, "FechaNacimiento").text.split('T')[0]
    except:
        nacimiento = None
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


tareas = ["crear_tablas", "distritos", "diputados", "votos2019"]
if __name__ == "__main__":
    for t in tareas:
        # O(n^2) pero a quién le importa
        if t in sys.argv:
            exec(t+"()")
