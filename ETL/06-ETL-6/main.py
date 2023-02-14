import pandas as pd
import mysql.connector
import soporte as sp


ata = sp.Subir_SQL("tiburones3", "AlumnaAdalab")

ata.crear_bbdd()

# Query para crear la tabla paises
tabla_paises = '''
CREATE TABLE IF NOT EXISTS `paises` (
  `country` VARCHAR (45) NOT NULL,
  `id_country` INT NOT NULL,
  PRIMARY KEY (`id_country`))
  ;'''

  # Creamos la query para crear la tabla clima, con una foreign key a paises
tabla_clima = '''
CREATE TABLE IF NOT EXISTS `clima` (
  `id_clima` INT NOT NULL AUTO_INCREMENT,
  `timepoint` INT NOT NULL,
  `cloudcover` INT NOT NULL,
  `highcloud` INT NOT NULL,
  `midcloud` INT NOT NULL,
  `lowcloud` INT NOT NULL,
  `rh_profile` VARCHAR (45) NOT NULL,
  `wind_profile` VARCHAR (45) NOT NULL,
  `temp2m` INT NOT NULL,
  `lifted_index` INT NOT NULL,
  `rh2m` INT NOT NULL,
  `msl_pressure` INT NOT NULL,
  `prec_type` VARCHAR (45) NOT NULL,
  `prec_amount` INT NOT NULL,
  `snow_depth` INT NOT NULL,
  `wind10m.direction` INT NOT NULL,
  `wind10m.speed` INT NOT NULL,
  `country` VARCHAR (45) NOT NULL,
  `latitud` DECIMAL NOT NULL,
  `longitud` DECIMAL NOT NULL,
  `id_country` INT NOT NULL,
  PRIMARY KEY (`id_clima`),
  INDEX `fk_ataques_paises1_idx` (`id_country` ASC) ,
  CONSTRAINT `fk_clima_paises` 
    FOREIGN KEY (`id_country`) 
    REFERENCES `paises`(`id_country`))
  ENGINE = InnoDB
  ;'''

  # Creamos la query para crear la tabla ataques, con una foreign key a paises
tabla_ataques = """
CREATE TABLE IF NOT EXISTS `ataques` (
    `id_ataques` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `year` INT NOT NULL,
    `type` VARCHAR(200) NOT NULL,
    `country` VARCHAR(45) NOT NULL,
    `activity` VARCHAR(200) NOT NULL,
    `age` INT NOT NULL,
    `species` VARCHAR(200) NOT NULL,
    `month` VARCHAR(200) NOT NULL,
    `fatal` VARCHAR(200) NOT NULL,
    `gender` VARCHAR(200) NOT NULL,
    `id_country` INT NOT NULL,
    INDEX `fk_ataques_paises1_idx` (`id_country` ASC) ,
    CONSTRAINT `fk_ataques_paises` 
        FOREIGN KEY (`id_country`) 
        REFERENCES `paises`(`id_country`))
    ENGINE = InnoDB
    ;"""


# Creamos las tablas
ata.crear_tabla(tabla_paises)
ata.crear_tabla(tabla_clima)
ata.crear_tabla(tabla_ataques)

df_ataques = pd.read_csv('../../data/06-tiburon_4.csv')

print("csv cargado")

# Iteramos por las filas del df ataques para ir insertando los datos en nuestra BBDD
for indice, fila in df_ataques.iterrows(): # iteramos por el dataframe.
    
    # definimos nuestra query, igual que si lo hicieramos en workbench. ⚠️ Como estamos definiendo nuestra query en varias líneas usamos las triples comillas
    # lo valores que introduciremos serán los del dataframe que estamos iterando, por lo que usaremos los formats de los strings. 
    
    query_ataques = f"""
            INSERT INTO ataques (year, type, country, activity, age, species, month, fatal, gender, id_country)
            VALUES ( "{fila['year']}", "{fila['type']}", "{fila['country']}", "{fila['activity']}", "{fila['age']}", "{fila['species']}", "{fila['month']}", "{fila['fatal']}", "{fila['gender']}", "{fila['id_country']}");
            """
    # una vez definida la query llamamos a la función que nos inserta los datos. 
    ata.insercion_datos(query_ataques)

