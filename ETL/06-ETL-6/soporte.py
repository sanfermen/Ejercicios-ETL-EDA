import pandas as pd
import mysql.connector

class Subir_SQL:
    def __init__(self, nombre_bbdd, contraseña):
        # nuestra clase va tener atributos, nombre de la BBDD y la contraseña 
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
    
    # Creamos la función para crear la base de datos
    def crear_bbdd(self):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password= f'{self.contraseña}' 
        )
        print("Conexión realizada con éxito")
        
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)


    # Creamos una funcion para crear nuestras tablas
    def crear_tabla(self, query):
    
    # nos conectamos con el servidor usando el conector de sql
        cnx = mysql.connector.connect(user='root', password=f"{self.contraseña}",
                                        host='127.0.0.1', database=f"{self.nombre_bbdd}")
        # iniciamos el cursor
        mycursor = cnx.cursor()
        
        # intentamos hacer la query
        try: 
            mycursor.execute(query)
            cnx.commit() 
        # en caso de que podamos ejecutar la query devuelvenos un error para saber en que nos estamos equivocando
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    # Creamos una función para insertar los datos
    def insercion_datos(self, query):
        cnx = mysql.connector.connect(user='root', password=f"{self.contraseña}",
                                    host='127.0.0.1', database=f"{self.nombre_bbdd}")
        mycursor = cnx.cursor()
        
        try: 
            # Le añadimos un escape para poder subir los datos a pesar de tener una foreign key
            mycursor.execute("SET foreign_key_checks = 0;")
            mycursor.execute(query)
            cnx.commit()
            mycursor.execute("SET foreign_key_checks = 1;")

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        