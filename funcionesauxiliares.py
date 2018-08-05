#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 23:37:15 2018

@author: enriquelazcorretapuigmarti
"""

import os, psutil, sys, hashlib, time

def tiempo_transcurrido(inicio, mensaje='', fin=None):
    if fin is None:
        fin = time.time()
    segundos = fin - inicio
            
    dias = 0
    horas = 0
    minutos = 0
    ms = int((segundos % 1) * 1000)
    segundos = int(segundos)
    
    resultado = mensaje
    
    if segundos >= 86400:
        dias = segundos // 86400
        segundos -= dias * 86400
    
    if segundos >= 3600:
        horas = segundos // 3600
        segundos -= horas * 3600
        
    if segundos >= 60:
        minutos = segundos // 60
        segundos -= minutos * 60
    
    if dias:
        resultado += '{:,}'.format(dias) + ' días '

    if horas:
        resultado += str(horas) + 'h '
        
    if minutos:
        resultado += str(minutos) + 'm '

    if segundos:
        resultado += str(segundos) + 's '
    
    if len(resultado):
        if ms:
            resultado += str(ms) + 'ms'
    else:
        resultado += str(ms) + 'ms'
    
    return resultado


def tamanyo_legible(tamanyo_en_bytes, si=False):
    if si:
        if tamanyo_en_bytes < 1024:
            return '{:}'.format(tamanyo_en_bytes) + 'B'
        
        unidad = ('KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB')
        base = 10
        exponente = (3, 6, 9, 12, 15, 18, 21, 24, 27)
    else:
        if tamanyo_en_bytes < 1024:
            return '{:}'.format(tamanyo_en_bytes) + 'B'
        
        unidad = ('KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB')
        base = 2
        exponente = (10, 20, 30, 40, 50, 60, 70, 80)

    for i in range(7):
        if tamanyo_en_bytes < base ** exponente[i+1]:
            return '{:.1f}'.format(tamanyo_en_bytes / (base ** exponente[i])) \
                    + unidad[i]
            
    return '{:.1f}'.format(tamanyo_en_bytes / (base ** exponente[7])) + 'YB'


def memoria_objeto(objeto, mensaje=''):
    return tamanyo_legible(sys.getsizeof(objeto)) + mensaje


#TODO Con memoria_objeto() no debería ser necesaria memoria_dataset()
#TODO Aunque memoria_objeto() es mucho más lento (22sg frente a 18sg al usar
#     datasetclasificacion-inplace con todos los archivos), HACER MÁS PRUEBAS.
def memoria_dataset(pandas_dataframe, mensaje=''):
    ram = 0
    for columna in pandas_dataframe.memory_usage():
        ram += columna
    return tamanyo_legible(ram) + mensaje


def memoria_proceso(mensaje=' RAM usada por el proceso'):
    pid = os.getpid()
    py = psutil.Process(pid)
    return tamanyo_legible(py.memory_info()[0]) + mensaje


def sha1_archivo(archivo):
    try:
        with open(archivo, 'rb') as file:
            return hashlib.sha1(file.read()).hexdigest()
    except:
        return 'No he podido abrir el archivo ' + archivo


def md5_archivo(archivo):
    try:
        with open(archivo, 'rb') as file:
            return hashlib.md5(file.read()).hexdigest()
    except:
        return 'No he podido abrir el archivo ' + archivo






if __name__ == '__main__':
    print(tiempo_transcurrido(10000.1236))
    t0 = time.time()
    time.sleep(2)
    print(tiempo_transcurrido(inicio=t0))
    t1 = time.time()
    print(tiempo_transcurrido(inicio=t0, fin=t1))
