#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 11:20:02 2018

@author: enriquelazcorretapuigmarti

TODO No parece funcionar bien con 'titanic', depurarlo a fondo.
"""


import time, os, configparser as cp, pandas as pd, pickle
from funcionesauxiliares import tiempo_transcurrido, memoria_dataset,\
                                memoria_proceso, sha1_archivo, tamanyo_legible


class InfoDC():
    """
    Las características del DC que interesan para ACDCv0.0 son:
        
        clase_al_final:
            posición de la clase en la evidencia, primer o último elemento.
        num_evidencias_dataset:
            Número de evidencias del DC, completas, incompletas y/o duplicadas.
        num_evidencias_completas:
            Número de evidencias completas del DC.
            En esta versión no se usan las evidencias incompletas.
        num_evidencias_catalogo:
            Número de evidencias completas diferentes del DC.
        num_evidencias_robustas:
            Número de evidencias robustas del DC.
        atributos_constantes:
            Lista de atributos constantes. Su posición en el DC
    """
    def __init__(self, dc, clase_al_final=True):
        self.num_evidencias, self.num_columnas = dc.shape
        self.num_atributos = self.num_columnas - 1
        
        self.clase = dc.columns[self.num_columnas-1] if clase_al_final\
                                                     else dc.columns[0]
        self.atributos = dc.columns[:self.num_columnas-1] if clase_al_final\
                                                          else dc.columns[1:]
        
        #TODO Seguro que hay un modo de obtener mejor resultado
        self.atributos_con_datos_desconocidos = []
        atributos_y_clase = dc.describe(include='all')
        for i, c in enumerate(atributos_y_clase.loc['count']):
            if c < self.num_evidencias:
                self.atributos_con_datos_desconocidos.append(dc.columns[i])
#                print('¡La columna {} tiene {:,} valores!'.format(i, c))
#                print('¡El atributo {} tiene {:,} valores!'.format(atributos_y_clase.columns[i], c))

        self.uso_memoria = 0
        for i in range(self.num_columnas):
            self.uso_memoria += dc.memory_usage(i, deep=True)[1]
#        self.uso_memoria = dc.memory_usage(deep=True)



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
        """Constructor:
        
        El DC se lee con pd.read_csv() y se guarda en un pd.DataFrame.
        
        Los DC descargados del repositorio KEEL, probados todos con este
        software, tienen la clase al final de la evidencia. En UCI no es
        siempre así, en mushroom tiene la clase al principio de la evidencia.
        Es necesario saber dónde está situada la clase en cualquier problema de
        clasificación. Se presentará una muestra del archivo al usuario para
        que pueda indicar dónde está la clase.
        
        Los valores desconocidos se marcan con '?' en UCI.
                
        **kw se podrá completar con las opciones de pandas.read_csv().
        """
        #TODO Aclarar si necesito estas variables,
        self.__dataset_leido = None
        self.__existen_evidencias_incompletas = None
        self.__existen_caracterizaciones_con_incertidumbre = None
        self.__tiene_atributos_constantes = None
        
        
        self.ruta_datasets = ruta_datasets
        self.nombre_dataset = nombre_dataset
        self.ruta_resultados = ruta_resultados
        self.archivo_original = ruta_datasets + '/' + nombre_dataset + '.csv'
        self.base_archivo_resultados = ruta_resultados + '/' + nombre_dataset
        #TODO Las extensiones deberían estar en una lista guardada en el
        #     archivo .cfg por si el usuario quiere modificarlas.
        
        self.mostrar_proceso = mostrar_proceso
        
        #TODO Falta parámetro valores_na='?'
        self.dataset = self.lee_dataset(num_filas_a_leer)
        
        if self.dataset is None:
            return

        self.__dataset_leido = False if num_filas_a_leer is None else True
        
        
        
        #TODO Crear un InfoDC distinto cada vez que cambie self.dataset.
        #TODO Eliminar todas las variables de DC cuando tenga todos los InfoDC.
        self.info_dataset_original = InfoDC(self.dataset, clase_al_final)

#        self.num_evidencias_dataset, self.num_columnas_dataset = \
#                                                             self.dataset.shape
        
        self.clase = self.dataset.columns[self.info_dataset_original.num_columnas-1] \
                     if clase_al_final else self.dataset.columns[0]
        self.atributos = self.dataset.columns[:self.info_dataset_original.num_columnas-1] \
                     if clase_al_final else self.dataset.columns[1:]
        self.num_atributos_dataset = len(self.atributos)


        if self.mostrar_proceso:
            print('DATASET ORIGINAL: {:,} x {:,}'.format(self.dataset.shape[0],
                                                      self.dataset.shape[1]-1))

        if mostrar_uso_ram:
            print('\t' + memoria_dataset(self.dataset, ' usados por el '\
                                        'dataset original'))
            print('\t' + memoria_proceso())

        if not obtener_catalogo_robusto:
            return

        self.elimina_evidencias_incompletas()
        if guardar_resultados:
            self.guarda_datos_desconocidos()
        borrar = self.lee_datos_desconocidos()
#        print(self.evidencias_con_datos_desconocidos.shape)
#        print(borrar.shape)
        del borrar

        if self.mostrar_proceso:
            num_evidencias_incompletas = self.info_dataset_original.num_evidencias -\
                                         self.num_evidencias_completas
            if num_evidencias_incompletas:
                print('\tEliminadas', '{:,}'.format(num_evidencias_incompletas),
                      'evidencias incompletas')
            else:
                print('\tNo hay evidencias incompletas')

            print('DATASET SIN DATOS DESCONOCIDOS: {:,} x {:,}'. \
                  format(self.dataset.shape[0], self.dataset.shape[1]-1))

        if mostrar_uso_ram:
            print('\t' + memoria_dataset(self.dataset, ' usados por el '\
                                  'dataset sin evidencias incompletas'))
            print('\t' + memoria_proceso())

        self.elimina_atributos_constantes()
        
        if self.mostrar_proceso:
            if self.num_atributos_constantes:
                print('\tQuedan',
                      self.info_dataset_original.num_columnas-self.num_atributos_constantes,
                      'atributos al eliminar',
                      self.num_atributos_constantes, 'atributos constantes')
            else:
                print('\tNo hay atributos constantes')

            print('DATASET SIN ATRIBUTOS CONSTANTES:', '{:,} x {:,}'.\
                  format(self.dataset.shape[0], self.dataset.shape[1]-1))

        if mostrar_uso_ram:
            print('\t' + memoria_dataset(self.dataset, ' usados por el '\
                              'dataset sin evidencias completas ni atributos '\
                              'constantes'))
            print('\t' + memoria_proceso())


        #TODO Debería guardar los índices de las evidencias duplicadas
        self.elimina_evidencias_duplicadas()

        if self.mostrar_proceso:
            num_evidencias_duplicadas = self.num_evidencias_completas -\
                                        self.num_evidencias_catalogo
            if num_evidencias_duplicadas:
                print('\tEliminadas', '{:,}'.format(num_evidencias_duplicadas),
                      'evidencias completas duplicadas')
            else:
                print('\tNo hay evidencias completas duplicadas')

            print('CATÁLOGO: {:,} x {:,}'.format(self.dataset.shape[0],
                                               self.dataset.shape[1]-1))

        if guardar_resultados:
            if self.mostrar_proceso:
                print('\tGuardando catálogo')

            self.guarda_catalogo()

        if mostrar_uso_ram:
            print('\t{}' + memoria_dataset(self.dataset,
                  ' usados por el catálogo'))
            print('\t' + memoria_proceso())

        
        #TODO Debería guardar los índices de las evidencias con incertidumbre
        self.elimina_evidencias_con_incertidumbre()

        self.num_evidencias_robustas = self.dataset.shape[0]
        self.num_evidencias_con_incertidumbre = self.num_evidencias_catalogo -\
                                                self.num_evidencias_robustas

        if self.mostrar_proceso:
            print('CATÁLOGO ROBUSTO: {:,} x {:,}'.format(self.dataset.shape[0],
                                                      self.dataset.shape[1]-1))

            if self.num_evidencias_con_incertidumbre:
                print('\tEliminadas', '{:,} evidencias completas con '\
                 'incertidumbre'.format(self.num_evidencias_con_incertidumbre))
            else:
                print('\tNo hay evidencias completas con incertidumbre')

        if mostrar_uso_ram:
            print('\t' + memoria_dataset(self.dataset, ' usados por el '\
                                         'catálogo robusto'))
            print('\t' + memoria_proceso())
        

        self.contiene_incertidumbre = True if \
                               self.num_evidencias_con_incertidumbre else False

        if guardar_resultados:
            if self.mostrar_proceso:
                print('\tGuardando catálogo robusto')

            self.guarda_catalogo_robusto()
            

        if guardar_datos_proyecto:
            self.guarda_datos_proyecto()

    def lee_dataset(self, num_filas_a_leer=None, valores_na='?'):
        try:
            #TODO Usar módulo os para determinar si añado '/' a la ruta
            dataset = pd.read_csv(self.archivo_original,
                                  na_values=valores_na,
                                  skipinitialspace=True,
                                  nrows=num_filas_a_leer)
        except Exception as e:
            print('####### ERROR ######\n', e, '\n####### ERROR ######')
            return None
        
        return dataset
        

    def elimina_evidencias_incompletas(self):
        #TODO Revisar otros métodos de obtener lo mismo: 
        #          pd.Index.difference(pd.Index)
        #     Primero crearía el índice de dataset y después de dropna lo
        #     vuelvo a crear y calculo la diferencia.
#        idx1 = pd.Index(self.dataset)
        self.evidencias_con_datos_desconocidos = \
                                    pd.isnull(self.dataset).any(1).nonzero()[0]
        
        self.dataset.dropna(inplace=True)
        self.num_evidencias_completas = self.dataset.shape[0]
#        idx2 = pd.Index(self.dataset)
#        print(idx1.difference(idx2).shape)
#        print(self.evidencias_con_datos_desconocidos.shape)
        #TODO En census indica 968 cuando se supone que hay 977 ev. incompletas
        #     En mushroom-UCI ambos son 2480 (correcto)



    def elimina_atributos_constantes(self):
        self.atributos_constantes = {}
        for indice, atributo in enumerate(self.dataset.columns):
            if len(self.dataset[atributo].unique()) == 1:
                if self.mostrar_proceso:
                    print('\t({}) {} es constante, con el valor {}'.\
                          format(indice, atributo,
                          self.dataset[atributo].unique()[0]))
                self.atributos_constantes[atributo] = indice
                self.dataset.drop(atributo, 1, inplace=True)
                self.atributos = self.atributos.drop(atributo)
        self.num_atributos_constantes = len(self.atributos_constantes)


    def elimina_evidencias_duplicadas(self):
#        idx1 = pd.Index(self.dataset)
        self.dataset.drop_duplicates(inplace=True)
        self.num_evidencias_catalogo, self.num_columnas_catalogo =\
                                                             self.dataset.shape
#        idx2 = pd.Index(self.dataset)
#        print(idx1.difference(idx2).shape)

    def elimina_evidencias_con_incertidumbre(self):
        self.dataset.drop_duplicates(self.atributos, keep=False, inplace=True)
        self.num_evidencias_catalogo_robusto, \
        self.num_columnas_catalogo_robusto = self.dataset.shape


    def guardar_resultados(self, guardar=None):
        if guardar is not None:
            self.guardar_resultados = guardar
        return self.guardar_resultados


    def guarda_catalogo(self):
        #TODO Quizá el nombre del archivo deba obtenerlo en una función
        archivo = self.base_archivo_resultados
        if self.num_atributos_constantes:
            for atributo in self.atributos_constantes:
                archivo += '-' + str(self.atributos_constantes[atributo])
        archivo += '.catalogo.csv'
        try:
            self.dataset.to_csv(archivo, sep=',', index=False)
            if self.mostrar_proceso:
                print('\t' + archivo)
        except Exception as e:
            print('####### ERROR ######\n', e, '\n####### ERROR ######')


    def guarda_catalogo_robusto(self):
        archivo = self.base_archivo_resultados
        if self.num_atributos_constantes:
            for atributo in self.atributos_constantes:
                archivo += '-' + str(self.atributos_constantes[atributo])
        archivo += '.catalogo-robusto.csv'
        try:
            self.dataset.to_csv(archivo, sep=',', index=False)
            if self.mostrar_proceso:
                print('\t' + archivo)
        except Exception as e:
            print('####### ERROR ######\n', e, '\n####### ERROR ######')


    def guarda_datos_proyecto(self):
        archivo_proyecto = cp.ConfigParser()
        archivo_proyecto.optionxform = lambda option: option
        
        archivo_proyecto['DEFAULT'] = {\
                'Ruta datasets': self.ruta_datasets,
                'Nombre dataset': self.nombre_dataset,
                'Ruta resultados': self.ruta_resultados,
                'sha1': sha1_archivo(self.archivo_original),
             'Tamaño': tamanyo_legible(os.path.getsize(self.archivo_original))}

        archivo_proyecto['DEFAULT']['Num evidencias'] = \
                                           str(self.info_dataset_original.num_evidencias)
        archivo_proyecto['DEFAULT']['Num atributos'] = \
                                         str(self.info_dataset_original.num_columnas - 1)
        archivo_proyecto['DEFAULT']['Clase'] = self.clase
        archivo_proyecto['DEFAULT']['Num evidencias incompletas'] = \
               str(self.info_dataset_original.num_evidencias - self.num_evidencias_completas)
        archivo_proyecto['DEFAULT']['Num atributos constantes'] = \
                                             str(self.num_atributos_constantes)
        archivo_proyecto['DEFAULT']['Num evidencias con incertidumbre'] = \
                                     str(self.num_evidencias_con_incertidumbre)

        if self.num_atributos_constantes:
            archivo_proyecto['Atributos constantes eliminados'] = {}
            for atributo in self.atributos_constantes:
                archivo_proyecto['Atributos constantes eliminados'][atributo] =\
                                       str(self.atributos_constantes[atributo])

        
        archivo_proyecto['Columnas catálogo'] = {}
        for columna in self.dataset.columns:
            archivo_proyecto['Columnas catálogo'][columna] = \
                                               str(self.dataset[columna].dtype)

        archivo_proyecto['Catálogo'] = {}
        archivo_proyecto['Catálogo']['Num evidencias'] = \
                                           str(self.num_evidencias_catalogo)
        archivo_proyecto['Catálogo']['Num atributos'] = \
                                         str(self.num_columnas_catalogo - 1)

        archivo_proyecto['Catálogo Robusto'] = {}
        archivo_proyecto['Catálogo Robusto']['Num evidencias'] = \
                                      str(self.num_evidencias_catalogo_robusto)
        archivo_proyecto['Catálogo Robusto']['Num atributos'] = \
                                    str(self.num_columnas_catalogo_robusto - 1)
        
        with open(self.base_archivo_resultados + '.cfg', 'w') as datos_proyecto:
            archivo_proyecto.write(datos_proyecto)
        

    def guarda_datos_desconocidos(self):
        with open(self.base_archivo_resultados + \
                  '.datos-desconocidos.acdc', 'wb') as archivo:
            pickle.dump(self.evidencias_con_datos_desconocidos, archivo)
        

    #TODO Experimental. Para leer el csv he de indicar qué filas saltar, no las
    #     que quiero leer
    def lee_datos_desconocidos(self):
        with open(self.base_archivo_resultados +
                  '.datos-desconocidos.acdc', 'rb') as archivo:
            self.evidencias_con_datos_desconocidos = pickle.load(archivo)
        filas_a_excluir = [i for i in range(self.info_dataset_original.num_evidencias) \
                           if i not in self.evidencias_con_datos_desconocidos]

        #TODO Encapsular en try/except.
        return pd.read_csv(self.archivo_original, skiprows=filas_a_excluir)


    #TODO self.dataset puede cambiar, tenerlo en cuenta.
    def muestra(self, num_filas):
        return self.dataset.head(num_filas)


    #TODO self.dataset puede cambiar, tenerlo en cuenta.
    def describe_atributos_y_clase(self):
        return self.dataset.describe(include='all')


    #TODO self.dataset puede cambiar, tenerlo en cuenta.
    def dataset_info(self):
        return self.dataset.info()


    def atributos_con_datos_desconocidos(self):
        total = self.info_dataset_original.num_evidencias
        atributos = []
        
        
        atributos_y_clase = self.dataset.describe(include='all')
        for i, c in enumerate(atributos_y_clase.loc['count']):
            if c < total:
                atributos.append(self.dataset.columns[i])
                print('\t¡El atributo {} ({}) tiene {:,} valores!'.format(atributos_y_clase.columns[i], i, c))
        
        
        
#        for i, c in enumerate(self.atributos[0]):
#            atributos.append((i, c))
#            if c < total:
#                atributos.append(self.dataset.columns[i])
#                print('¡La columna {} tiene {} valores!'.format(i, c))
#                print('¡El atributo {} tiene {} valores!'.format(atributos_y_clase.columns[i], c))
        return atributos





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
    for i, nombre_dataset in enumerate(['abalone']):
#TODO En http://sci2s.ugr.es/keel/dataset.php?cod=52 dicen:
#           "In this version, 3 duplicated instances have been removed from the 
#            original UCI dataset."
#     Pero yo no encuentro duplicados en abalone-UCI, que sí tiene 3 más.
#    for i, nombre_dataset in enumerate(['abalone-UCI']):
#    for i, nombre_dataset in enumerate(['adult']):
#    for i, nombre_dataset in enumerate(['adult-UCI']):
#    for i, nombre_dataset in enumerate(['adult-con-test-UCI']):
#    for i, nombre_dataset in enumerate(['balloons']):
#    for i, nombre_dataset in enumerate(['census']):
#    for i, nombre_dataset in enumerate(['hepatitis']):
#    for i, nombre_dataset in enumerate(['kddcup']):
#    for i, nombre_dataset in enumerate(['kddcup99']):
#    for i, nombre_dataset in enumerate(['monk-2']):
#    for i, nombre_dataset in enumerate(['mushroom']):
#    for i, nombre_dataset in enumerate(['mushroom-UCI']):
#    for i, nombre_dataset in enumerate(['titanic']):
#    for i, nombre_dataset in enumerate(['tae']):
#        if i < len(archivos_KEEL) // 2 + 1:
#            continue

        t1 = time.time()
        
        ruta_datasets = '../datos/ACDC/'
        ruta_resultados = '../datos/catalogos/'
        #TODO No puedo gestionar así los datasets con clase al principio,
        #     debería crear una tupla con los valores COMPROBADOS. Tengo
        #     adult-UCI con la clase al final y mushroom-UCI al principio
#        clase_al_final = False if '-UCI' in nombre_dataset else True
        clase_al_final = True
        guardar_resultados = False
        num_filas_a_leer = None
        mostrar_proceso = True
        mostrar_tiempos = False
        mostrar_uso_ram = False
        obtener_catalogo_robusto = True
        guardar_datos_proyecto = False
        
        if mostrar_proceso:
            print('\n###', i+1, nombre_dataset)
        else:
            print('\b\b\b\b', end='')
            print('{:.0%}'.format(i/num_archivos), end='')

        dc = DatasetClasificacion(ruta_datasets,
                                  nombre_dataset, 
                                  ruta_resultados,
                                  guardar_resultados=guardar_resultados,
                                  clase_al_final=clase_al_final,
                                  mostrar_proceso=mostrar_proceso,
                                  num_filas_a_leer=num_filas_a_leer,
                                  obtener_catalogo_robusto=obtener_catalogo_robusto,
                                  guardar_datos_proyecto=guardar_datos_proyecto,
                                  mostrar_uso_ram=mostrar_uso_ram)
        
        try:
            if dc.contiene_incertidumbre:
                num_datasets_con_incertidumbre += 1
            else:
                num_datasets_robustos += 1
        except:
            pass

        if mostrar_proceso:
            print(tiempo_transcurrido(time.time() - t1,
                                      'Tiempo de ejecución: '))
        else:
            print('\b\b\b\b', end='')
            print('{:.0%}'.format((i+1)/num_archivos), end='')
        
#        if i == len(archivos_KEEL) // 2:
#            break

    print()
    print(num_datasets_con_incertidumbre, 'datasets con incertidumbre')
    print(num_datasets_robustos, 'datasets robustos')
    print(tiempo_transcurrido(time.time() - t0,
                              '\nTiempo TOTAL de ejecución: '))
