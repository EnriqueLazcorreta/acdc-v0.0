#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 11:20:02 2018

@author: enriquelazcorretapuigmarti

TODO No parece funcionar bien con 'titanic', depurarlo a fondo.
"""


import time, os, configparser as cp, pandas as pd, pickle, numpy as np
from funcionesauxiliares import tiempo_transcurrido, memoria_dataset,\
                                memoria_proceso, sha1_archivo, tamanyo_legible


class InfoDC():
    """
    En ACDCv0.0 partimos de un DC leído desde disco y lo vamos reduciendo para
    conocer sus características respecto a sus caracterizaciones y clase. La
    reducción se hace sobre el mismo pd.DataFrame, haciendo copias
    necesitaríamos demasiados recursos de memoria en datasets grandes. Para no
    perder las características de cada uno de los datasets reducidos las
    guardaremos en objetos InfoDC de los datasets:
        
        - D_0 = Dataset original
        - D_1 = D_0 sin datos desconocidos (sin evidencias incompletas)
        - D_2 = D_1 sin atributos constantes 
        - D_3 = D_2 sin duplicados
        - D_4 = D_3 sin incertidumbre (Máximo Catálogo Robusto de D_0)
    
    La información que necesito sobre todos estos datasets se obtiene con
    pd.dataset.describe(). Esta clase se especializará en obtener información
    específica del pd.DataFrame obtenido con describe().
    """
    def __init__(self, dc, clase):
        self.clase = clase
        self.columnas = dc.describe(include='all')


    def num_evidencias(self):
        return int(self.columnas[self.clase]['count'])


    def num_atributos(self):
        return int(len(self.columnas.columns) - 1)
    
    
    def atributos(self):
        return [columna for columna in self.columnas.columns \
                if columna != self.clase]



class DatasetClasificacion():
    """
    Un dataset de clasificación (DC) contiene evidencias, caracterizaciones
    clasificadas (supuestamente bien). Pueden ser evidencias completas o tener
    valores desconocidos en su caracterización (valores que toman los atributos
    en la evidencia).
    
    En algunos casos, un DC contiene atributos con un único valor, atributos
    a los que llamamos constantes. Esta información no sirve para el proceso
    de clasificación y duplica el número de resultados por lo que se eliminan.
    
    Un Catálogo Robusto (CR) sólo contiene evidencias completas sin
    incertidumbre, y no contiene atributos constantes.
    
    Esta clase recibe un DC y obtiene su MCR, el Máximo Catálogo Robusto que
    contiene.
            
    A modo descriptivo, para futuras versiones:
        
        conjunto_incertidumbre:
            Evidencias con incertidumbre en el DC.
        conjunto_incompleto:
            Evidencias incompletas en el DC.
    
    Los archivos csv que contienen el DC están ubicados en la misma carpeta,
    los resultados obtenidos se guardan (opcionalmente) en otra carpeta.
    
    TODO Decidir con JuanFran si añadimos la opción de determinar el tipo de
         atributos (redundantes/esenciales) respecto al dataset inicial.
    
    TODO Añadir flags que indiquen qué se ha procesado y qué no:
        
        - 1  dataset_leido
        - 2  existen_evidencias_incompletas
        - 4  existen_caracterizaciones_con_incertidumbre
        - 8  tiene_atributos_constantes
        - 16 
    """
    
    def __init__(self, ruta_datasets, nombre_dataset, ruta_resultados,
                 guardar_resultados=True, clase_al_final=True,
                 mostrar_proceso=True, num_filas_a_leer=None,
                 obtener_catalogo_robusto=True, guardar_datos_proyecto=True,
                 mostrar_uso_ram=False, **kw):
        """
        El DC se lee con pd.read_csv() y se guarda en un pd.DataFrame.
        
        Los DC descargados del repositorio KEEL, probados todos con este
        software, tienen la clase al final de la evidencia. En UCI no es
        siempre así, en mushroom tiene la clase al principio de la evidencia.
        Es necesario saber dónde está situada la clase en cualquier problema de
        clasificación. Se presentará una muestra del archivo al usuario para
        que pueda indicar dónde está la clase.
        
        Los valores desconocidos se marcan con '?' en UCI.
        
        Propiedades:
            
            - clase_al_final: boolean
            - ruta_datasets: string
            - nombre_dataset: string
            - ruta_resultados: string
            - archivo_original: string con ruta, nombre y extensión del DC
            - base_archivo_resultados: string con ruta y nombre, sin extensión
            - dataset: pd.DataFrame con los distintos datasets del proceso
            - clase: string
            - info_dataset_original: InfoDC
            - evidencias_incompletas: numpy.ndarray

        
        **kw se podrá completar con las opciones de pandas.read_csv().
        """
        #TODO Aclarar si necesito estas variables,
        self.__dataset_leido = None
        self.__existen_evidencias_incompletas = None
        self.__existen_caracterizaciones_con_incertidumbre = None
        self.__tiene_atributos_constantes = None
        
        self.clase_al_final = clase_al_final

        self.ruta_datasets = ruta_datasets if ruta_datasets[-1] in '/\\'\
                               else ruta_datasets + '/'
        self.nombre_dataset = nombre_dataset
        self.ruta_resultados = ruta_resultados if ruta_resultados[-1] in '/\\'\
                               else ruta_resultados + '/'
        """
        TODO Las extensiones deberían estar en una lista guardada en el archivo
             .cfg por si el usuario quiere modificarlas."""
        self.archivo_original = self.ruta_datasets + nombre_dataset + '.csv'
        self.base_archivo_resultados = self.ruta_resultados + nombre_dataset
        
        self.mostrar_proceso = mostrar_proceso
        
        #TODO Falta parámetro valores_na='?'
        self.dataset = self.lee_dataset(num_filas_a_leer)
        
        if self.dataset is None:
            return

        self.__dataset_leido = False if num_filas_a_leer is None else True
        
        self.clase = self.dataset.columns[self.dataset.shape[1]-1] \
                     if clase_al_final else self.dataset.columns[0]

        #TODO Crear un InfoDC distinto cada vez que cambie self.dataset.
        #TODO Eliminar todas las variables de DC cuando tenga todos los InfoDC.
        self.info_dataset_original = InfoDC(self.dataset, self.clase)
        
        if self.mostrar_proceso:
            print('{:,} x {:,} (DATASET ORIGINAL)'.format(self.dataset.shape[0],
                                                      self.dataset.shape[1]-1))
        if mostrar_uso_ram:
            print('\t{} usados por el dataset original'.\
                  format(memoria_dataset(self.dataset)))
            print('\t{} usados por el proceso'.format(memoria_proceso()))
        self.elimina_evidencias_incompletas()
        self.info_dataset_sin_datos_desconocidos = \
                                           InfoDC(self.dataset, self.clase)
        
        if self.mostrar_proceso:
            num_evidencias_incompletas = \
                      self.info_dataset_original.num_evidencias() - \
                      self.info_dataset_sin_datos_desconocidos.num_evidencias()
            if num_evidencias_incompletas:
                print('\tEliminadas {:,} evidencias incompletas'. \
                      format(num_evidencias_incompletas))
            else:
                print('\tNo hay evidencias incompletas')
            print('{:,} x {:,} (DATASET SIN DATOS DESCONOCIDOS)'. \
                  format(self.dataset.shape[0], self.dataset.shape[1]-1))

        if mostrar_uso_ram:
            print('\t{} usados por el dataset sin evidencias incompletas'. \
                  format(memoria_dataset(self.dataset)))
            print('\t{} usados por el proceso'.format(memoria_proceso()))

        self.elimina_atributos_constantes()
        self.info_dataset_sin_atributos_constantes = \
                                           InfoDC(self.dataset, self.clase)
        
        if self.mostrar_proceso:
            num_atributos_constantes = \
                self.info_dataset_original.num_atributos() - \
                self.info_dataset_sin_atributos_constantes.num_atributos()
            if not num_atributos_constantes:
                print('\tNo hay atributos constantes')
            print('{:,} x {:,} (DATASET SIN ATRIBUTOS CONSTANTES)'.\
                  format(self.dataset.shape[0], self.dataset.shape[1]-1))
        if mostrar_uso_ram:
            print('\t{} usados por el dataset sin evidencias incompletas ' \
                  'ni atributos constantes'.\
                  format(memoria_dataset(self.dataset)))
            print('\t{} usados por el proceso'.format(memoria_proceso()))


        #TODO Guardar los índices de las evidencias duplicadas
        self.elimina_evidencias_duplicadas()
        self.info_catalogo = InfoDC(self.dataset, self.clase)

        if self.mostrar_proceso:
            num_evidencias_duplicadas = \
                     self.info_dataset_sin_datos_desconocidos.num_evidencias() -\
                     self.info_catalogo.num_evidencias()
            if num_evidencias_duplicadas:
                print('\tEliminadas {:,} evidencias completas duplicadas'. \
                      format(int(num_evidencias_duplicadas)))
            else:
                print('\tNo hay evidencias completas duplicadas')

            print('{:,} x {:,} (CATÁLOGO)'.format(self.dataset.shape[0],
                                               self.dataset.shape[1]-1))
        if mostrar_uso_ram:
            print('\t{} usados por el catálogo'.\
                  format(memoria_dataset(self.dataset)))
            print('\t{} usados por el proceso'.format(memoria_proceso()))

        
        self.elimina_evidencias_con_incertidumbre()
        self.info_catalogo_robusto = InfoDC(self.dataset, self.clase)

        num_evidencias_con_incertidumbre = self.info_catalogo.num_evidencias()\
                                  - self.info_catalogo_robusto.num_evidencias()

        if self.mostrar_proceso:
            print('{:,} x {:,} (CATÁLOGO ROBUSTO)'.format(self.dataset.shape[0],
                                                      self.dataset.shape[1]-1))
            if num_evidencias_con_incertidumbre:
                print('\tEliminadas {:,} evidencias completas con '\
                 'incertidumbre'.format(num_evidencias_con_incertidumbre))
            else:
                print('\tNo hay evidencias completas con incertidumbre')
        if mostrar_uso_ram:
            print('\t{} usados por el catálogo robusto'.\
                  format(memoria_dataset(self.dataset)))
            print('\t{} usados por el proceso'.format(memoria_proceso()))
        
        self.contiene_incertidumbre = True if \
                                    num_evidencias_con_incertidumbre else False

        if guardar_resultados:
            if self.mostrar_proceso:
                print('\tGuardando catálogo robusto')
            self.guarda_catalogo_robusto()

        if guardar_datos_proyecto:
            if self.mostrar_proceso:
                print('\tGuardando datos proyecto')
            self.guarda_datos_proyecto()
            
#        self.atributos_con_datos_desconocidos()
        print(self.atributos_con_datos_desconocidos())
        print(self._notacion_D_I(self.atributos_constantes))


    def lee_dataset(self, num_filas_a_leer=None, valores_na='?'):
        try:
            dataset = pd.read_csv(self.archivo_original,
                                  na_values=valores_na,
                                  skipinitialspace=True,
                                  nrows=num_filas_a_leer)
        except Exception as e:
            print('####### ERROR ######\n{}\n####### ERROR ######'.format(e))
            return None
        
        return dataset
        

    def elimina_evidencias_incompletas(self):
        """Guarda el índice de las evidencias incompletas y las elimina"""
        self.evidencias_incompletas = \
                                    pd.isnull(self.dataset).any(1).nonzero()[0]
        self.dataset.dropna(inplace=True)


    def elimina_atributos_constantes(self):
        self.atributos_constantes = {}
        for indice, atributo in enumerate(self.dataset.columns):
            if len(self.dataset[atributo].unique()) == 1:
                if self.mostrar_proceso:
                    print('\t({}) {} es constante, con el valor {}'.\
                          format(indice, atributo,
#                          format(self._indice(indice), atributo,
                          self.dataset[atributo].unique()[0]))
                self.atributos_constantes[atributo] = indice
                self.dataset.drop(atributo, 1, inplace=True)


    #TODO Una vez tengo indices_duplicados ¿Es más rápido usarlo para eliminar duplicados del DC?
    def elimina_evidencias_duplicadas(self):
        indices_duplicados = self.dataset.duplicated()
        self.lista_indices_duplicados = str([indice for indice, valor in indices_duplicados.iteritems() if valor])
        self.dataset.drop_duplicates(inplace=True)


    """
    TODO Una vez tengo indices_con_incertidumbre ¿Es más rápido usarlo para 
         eliminar incertidumbre del DC?"""
    def elimina_evidencias_con_incertidumbre(self):
        atributos = self.info_dataset_sin_atributos_constantes.atributos()
        self.indices_con_incertidumbre = self.dataset.duplicated(atributos,
                                                                 keep=False)
        self.dataset.drop_duplicates(atributos, keep=False, inplace=True)


    def guarda_resultado(self, archivo):
        try:
            self.dataset.to_csv(archivo, sep=',', index=False)
            if self.mostrar_proceso:
                print('\t\t' + archivo)
        except Exception as e:
            print('####### ERROR ######\n', e, '\n####### ERROR ######')


    def guarda_catalogo_robusto(self):
        self.guarda_resultado(self._notacion_D_I(self.atributos_constantes,
                                                 '.catalogo-robusto.csv'))


    def guarda_datos_proyecto(self):
        archivo_proyecto = cp.ConfigParser()
        archivo_proyecto.optionxform = lambda option: option
        
        archivo_proyecto['General'] = {\
            'Ruta datasets': self.ruta_datasets,
            'Ruta resultados': self.ruta_resultados}

        dc_original = self.info_dataset_original
        dc_sin_dd = self.info_dataset_sin_datos_desconocidos
        archivo_proyecto['Dataset de Clasificación'] = {\
            'Nombre dataset': self.nombre_dataset,
            'sha1': sha1_archivo(self.archivo_original),
            'Tamaño': tamanyo_legible(os.path.getsize(self.archivo_original)),
            'Clase al final': self.clase_al_final,
            'Num evidencias': str(dc_original.num_evidencias()),
            'Num atributos': str(dc_original.num_atributos()),
            'Clase': dc_original.clase,
            'Num evidencias incompletas': str(dc_original.num_evidencias() - \
                                              dc_sin_dd.num_evidencias()),
            'Evidencias incompletas': str(list(self.evidencias_incompletas)),
            'Num atributos constantes': str(len(self.atributos_constantes)),
            'Evidencias completas duplicadas (eliminadas)': \
                                                 self.lista_indices_duplicados}

        if len(self.atributos_constantes):
            archivo_proyecto['Atributos constantes eliminados'] = {}
            for atributo in self.atributos_constantes:
                archivo_proyecto['Atributos constantes eliminados'][atributo]=\
                                       str(self.atributos_constantes[atributo])

        #TODO Debería guardar ¿también? las columnas del DC original
        archivo_proyecto['Columnas catálogo'] = {}
        for columna in self.dataset.columns:
            archivo_proyecto['Columnas catálogo'][columna] = \
                                               str(self.dataset[columna].dtype)

        num_evidencias_con_incertidumbre = self.info_catalogo.num_evidencias()\
                                  - self.info_catalogo_robusto.num_evidencias()
        archivo_proyecto['Catálogo'] = {\
            'Num evidencias': str(self.info_catalogo.num_evidencias()),
            'Num atributos': str(self.info_catalogo.num_atributos()),
            'Num evidencias con incertidumbre': \
                                    str(int(num_evidencias_con_incertidumbre)),
            'Evidencias con incertidumbre': str([indice for indice, valor \
                      in self.indices_con_incertidumbre.iteritems() if valor])}

        archivo_proyecto['Catálogo Robusto'] = {\
            'Num evidencias': str(self.info_catalogo_robusto.num_evidencias()),
            'Num atributos': str(self.info_catalogo_robusto.num_atributos())}
        
        with open(self.base_archivo_resultados+'.cfg', 'w') as archivo:
            archivo_proyecto.write(archivo)

        if self.mostrar_proceso:
            print('\t\t' + self.base_archivo_resultados + '.cfg')
        

    """
    TODO self.dataset puede cambiar, el resultado depende del momento en que
         se llame a la función. Debería llamarse muestra_catalogo_robusto() si
         se llama fuera de la clase y obtener_catalogo_robusto=True"""
    def muestra(self, num_filas):
        return self.dataset.head(num_filas)


    def atributos_con_datos_desconocidos(self):
        total = self.info_dataset_original.num_evidencias()
        atributos = []
        
        atributos_y_clase = self.info_dataset_original.columnas
        for i, c in enumerate(atributos_y_clase.loc['count']):
            if c < total:
                atributos.append(atributos_y_clase.columns[i])
                if self.mostrar_proceso:
                    print('\t¡El atributo {} ({}) tiene {:,} valores!'. \
                      format(atributos_y_clase.columns[i], self._indice(i), c))
#                             i+1 if self.clase_al_final else i, c))
        return atributos


    def _notacion_D_I(self, I, extension='.csv'):
        archivo = self.base_archivo_resultados
        for indice in I:
            archivo += '-' + str(self._indice(I[indice]))
        return archivo + extension


    def _indice(self, i):
        return i+1 if self.clase_al_final else i



if __name__ == '__main__':
    archivos_KEEL = ('abalone', 'adult', 'appendicitis', 'australian',
                     'automobile', 'balance', 'banana', 'bands', 'breast',
                     'bupa', 'car', 'census', 'chess', 'cleveland', 'coil2000',
                     'connect-4', 'contraceptive', 'crx', 'dermatology',
                     'ecoli', 'fars', 'flare', 'german', 'glass', 'haberman',
                     'hayes-roth', 'heart', 'hepatitis', 'housevotes',
                     'ionosphere', 'iris', 'kddcup', 'kr-vs-k', 'led7digit',
                     'letter', 'lymphography', 'magic', 'mammographic',
                     'marketing', 'monk-2', 'movement_libras', 'mushroom',
                     'newthyroid', 'nursery', 'optdigits', 'page-blocks',
                     'penbased', 'phoneme', 'pima', 'poker', 'post-operative',
                     'ring', 'saheart', 'satimage', 'segment', 'shuttle',
                     'sonar', 'spambase', 'spectfheart', 'splice', 'tae',
                     'texture', 'thyroid', 'tic-tac-toe', 'titanic', 'twonorm',
                     'vehicle', 'vowel', 'wdbc', 'wine', 'winequality-red',
                     'winequality-white', 'wisconsin', 'yeast', 'zoo')
    
    num_datasets_con_incertidumbre, num_datasets_robustos = 0, 0
    
    t0 = time.time()
    
    num_archivos = len(archivos_KEEL)

#    for i, nombre_dataset in enumerate(archivos_KEEL):
#    for i, nombre_dataset in enumerate(['abalone']):
#TODO En http://sci2s.ugr.es/keel/dataset.php?cod=52 dicen:
#           "In this version, 3 duplicated instances have been removed from the 
#            original UCI dataset."
#     Pero yo no encuentro duplicados en abalone-UCI, que sí tiene 3 más.
#    for i, nombre_dataset in enumerate(['abalone-UCI']):
#    for i, nombre_dataset in enumerate(['adult']):
#    for i, nombre_dataset in enumerate(['adult-UCI']):
#    for i, nombre_dataset in enumerate(['adult-con-test-UCI']):
#    for i, nombre_dataset in enumerate(['balloons']):
#    for i, nombre_dataset in enumerate(['breast']):
#    for i, nombre_dataset in enumerate(['census']):
#    for i, nombre_dataset in enumerate(['hepatitis']):
#    for i, nombre_dataset in enumerate(['kddcup']):
    for i, nombre_dataset in enumerate(['kddcup99']):
#    for i, nombre_dataset in enumerate(['monk-2']):
#    for i, nombre_dataset in enumerate(['mushroom']):
#    for i, nombre_dataset in enumerate(['mushroom-UCI']):
#    for i, nombre_dataset in enumerate(['titanic']):
#    for i, nombre_dataset in enumerate(['tae']):
#        if i < len(archivos_KEEL) // 2 + 1:
#            continue

        t1 = time.time()
        
        ruta_datasets = '../datos/ACDC/'
        ruta_resultados = '../datos/catalogos'
        clase_al_final = True
#        clase_al_final = False
        guardar_resultados = False
        num_filas_a_leer = None
        mostrar_proceso = False
        mostrar_tiempos = False
        mostrar_uso_ram = False
        obtener_catalogo_robusto = True
        guardar_datos_proyecto = False
        
        if mostrar_proceso:
            print('\n###', i+1, nombre_dataset)
        else:
            print('\b\b\b\b\b\b\b\b', end='')
            print('{:.0%}'.format(i/num_archivos), end='')

        try:
            dc = DatasetClasificacion(ruta_datasets,
                                      nombre_dataset, 
                                      ruta_resultados,
                                      guardar_resultados=guardar_resultados,
                                      clase_al_final=clase_al_final,
                                      mostrar_proceso=mostrar_proceso,
                                      num_filas_a_leer=num_filas_a_leer,
                                      obtener_catalogo_robusto=\
                                          obtener_catalogo_robusto,
                                      guardar_datos_proyecto=\
                                          guardar_datos_proyecto,
                                      mostrar_uso_ram=mostrar_uso_ram)
        except Exception as e:
            print('### EXCEPCIÓN 1 ###\n{}'.format(e))
                  
        try:
            if dc.contiene_incertidumbre:
                num_datasets_con_incertidumbre += 1
            else:
                num_datasets_robustos += 1
        except Exception as e:
            print('### EXCEPCIÓN 2 ###\n{}'.format(e))

        if mostrar_proceso:
            print(tiempo_transcurrido(t1, 'Tiempo de ejecución: '))
        else:
            print('\b\b\b\b', end='')
            print('{:.0%}'.format((i+1)/num_archivos), end='')
        
    print()
    print(num_datasets_con_incertidumbre, 'datasets con incertidumbre')
    print(num_datasets_robustos, 'datasets robustos')
    print(tiempo_transcurrido(t0, '\nTiempo TOTAL de ejecución: '))
