#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Tue Jul 31 09:56:17 2018

@author: enriquelazcorretapuigmarti
'''

from tkinter import Tk, Menu, StringVar, Text, Listbox, Button, BooleanVar, \
                    Toplevel
from tkinter.ttk import LabelFrame, Scrollbar, Label, Style, Treeview, \
                        Progressbar
from tkinter.filedialog import askopenfilename, askdirectory
import pandas as pd, time, os, sys
from configparser import ConfigParser
from funcionesauxiliares import tiempo_transcurrido, tamanyo_legible
from datasetclasificacion import DatasetClasificacion as DC
from tkinter.simpledialog import askinteger
#from multiprocessing import Process, Queue

APP_NAME = 'ACDCv0.0'
APP_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

#Para mostrar información sobre todas las columnas del catálogo
pd.set_option('display.max_columns', None)


class GUIDatasetClasificacion():
    def __init__(self, root):   
        self.root = root
        
        #TODO Esta información se lee desde .cfg con ConfigParser
        self._ruta_datasets = '../datos/ACDC/'
        self._ruta_resultados = '../datos/catalogos/'
        self._rutas_relativas = BooleanVar(root, True)
        self._usa_sha1 = BooleanVar(root, True)
        self._tamanyo_muestra = 5

        self._clase_al_final = BooleanVar(root, True)
#        self._clase_al_final = BooleanVar(root, False)
        
        self._mostrar_proceso = BooleanVar(root, False)
        
        estilo_bien = Style()
        estilo_bien.configure('G.TLabel', foreground='green')
        estilo_mal = Style()
        estilo_mal.configure('R.TLabel', foreground='red')
        
        self.crea_GUI()
        
        self.root.protocol('WM_DELETE_WINDOW', self.cerrar_aplicacion)
        self.root.title(APP_NAME)
        self.root.update()
        self.root.minsize(self.root.winfo_width(), self.root.winfo_height())


    def crea_GUI(self):
        root = self.root
        #Menús
        self.menubar = Menu(root, tearoff=0)
        
        m_archivo = Menu(self.menubar, tearoff=0)
        m_archivo.add_command(label='Abrir', command=self.abrir_dataset,
                              accelerator='Ctrl+O')
        m_archivo.add_separator()
        m_archivo.add_command(label='Salir', command=self.cerrar_aplicacion)
        self.menubar.add_cascade(label='Archivo', menu=m_archivo)
        
        m_proyecto = Menu(self.menubar, tearoff=0)
        m_proyecto.add_command(label='Abrir', command=self.abrir_proyecto)
        m_proyecto.add_separator()
        m_proyecto.add_checkbutton(label='Clase al final',
                                   onvalue=True, offvalue=False,
                                   variable=self._clase_al_final)
        self.menubar.add_cascade(label='Proyecto', menu=m_proyecto)

        self.m_configuracion = Menu(self.menubar, tearoff=0)
        self.m_configuracion.add_command(label='Ruta datasets',
                                    command=lambda: self.rutas('datasets'))
        self.m_configuracion.add_command(label='Ruta resultados',
                                    command=lambda: self.rutas('resultados'))
        self.m_configuracion.add_checkbutton(label='Rutas relativas',
                                        onvalue=True, offvalue=False,
                                        variable=self._rutas_relativas)
        self.m_configuracion.add_separator()
        #TODO Revisar self.v_tamanyo_muestra, no la uso
#        self.v_tamanyo_muestra = StringVar(root, 'Tamaño muestra ({:,})'.\
#                                           format(self._tamanyo_muestra))
        self.m_cfg_tamanyo_muestra = \
            self.m_configuracion.add_command(label='Tamaño muestra ({:,})'.\
                                           format(self._tamanyo_muestra),
                                        command=lambda: self.tamanyo_muestra(\
                                                        self._tamanyo_muestra))
        self.m_configuracion.add_separator()
        self.m_configuracion.add_checkbutton(label='Utiliza sha1',
                                        onvalue=True, offvalue=False,
                                        variable=self._usa_sha1)
        self.menubar.add_cascade(label='Configuración', menu=self.m_configuracion)
        
        m_ver = Menu(self.menubar, tearoff=0)
        m_ver.add_checkbutton(label='Log del proceso',
                              onvalue=True, offvalue=False,
                              variable=self._mostrar_proceso)
        self.menubar.add_cascade(label='Ver', menu=m_ver)
        
        root.config(menu=self.menubar)
        
        
        #Dataset de clasificación
        lf_dataset = LabelFrame(root, text='Dataset de Clasificación')
        lf_dataset.pack(fill='both', expand=True, padx=5, pady=5)
        
        Label(lf_dataset, text='Nombre:').grid(row=0, column=0, sticky='e')
        self.v_nombre_dataset = StringVar(root, '-------')
        self.l_nombre_dataset = Label(lf_dataset,
                                      textvariable=self.v_nombre_dataset)
        self.l_nombre_dataset.grid(row=0, column=1, sticky='w')

        Label(lf_dataset, text='Tamaño:').grid(row=0, column=2, sticky='e')
        self.v_tamanyo_dataset = StringVar(root, '-------')
        Label(lf_dataset, textvariable=self.v_tamanyo_dataset).grid(row=0,
             column=3, sticky='w')

        Label(lf_dataset, text='Ubicación:').grid(row=1, column=0, sticky='e')
        self.v_ruta_dataset = StringVar(root, '-------------------------')
        #TODO Expandir en columnas 1-3, puede ser muy larga
        Label(lf_dataset, textvariable=self.v_ruta_dataset).grid(row=1,
             column=1, sticky='w', columnspan=3)
        
        #Dataset de clasificación / Muestra
        lf_dataset_muestra = LabelFrame(lf_dataset, text='Muestra')
        lf_dataset_muestra.grid(row=2, column=0, sticky='nsew', columnspan=4,
                                padx=5, pady=5)

        self.sb_v_t_muestra = Scrollbar(lf_dataset_muestra)
        self.sb_v_t_muestra.grid(row=0, column=1, sticky='sn')
        
        self.sb_h_t_muestra = Scrollbar(lf_dataset_muestra, orient='horizontal')
        self.sb_h_t_muestra.grid(row=1, column=0, sticky='ew')
        
        self.t_muestra = Text(lf_dataset_muestra,
                              yscrollcommand=self.sb_v_t_muestra.set,
                              xscrollcommand=self.sb_h_t_muestra.set, bd=0,
                              wrap='none', state='disabled', height=8)
        self.t_muestra.grid(row=0, column=0, sticky='nswe')
        
        self.sb_v_t_muestra.config(command=self.t_muestra.yview)
        self.sb_h_t_muestra.config(command=self.t_muestra.xview)
        
        lf_dataset_muestra.rowconfigure(0, weight=1)
        lf_dataset_muestra.columnconfigure(0, weight=1)

        lf_dataset.rowconfigure(2, weight=3)
        lf_dataset.columnconfigure(1, weight=1)
        lf_dataset.columnconfigure(3, weight=1)


        #Dataset de clasificación / Evidencias
        lf_dataset_evidencias = LabelFrame(lf_dataset, text='Evidencias')
        lf_dataset_evidencias.grid(row=3, column=0, sticky='nsew', padx=5,
                                   pady=5)
        
        Label(lf_dataset_evidencias, text='Total:').grid(row=0, column=0,
                                                         sticky='e')
        self.v_evidencias_total = StringVar(root, '-------')
        Label(lf_dataset_evidencias, textvariable=self.v_evidencias_total).\
              grid(row=0, column=1, sticky='w')
        Label(lf_dataset_evidencias, text='Completas:').grid(row=1, column=0,
                                                         sticky='e')
        self.v_evidencias_completas = StringVar(root, '-------')
        Label(lf_dataset_evidencias, textvariable=self.v_evidencias_completas).\
              grid(row=1, column=1, sticky='w')
        Label(lf_dataset_evidencias, text='Únicas:').grid(row=2, column=0,
                                                          sticky='e')
        self.v_evidencias_catalogo = StringVar(root, '-------')
        Label(lf_dataset_evidencias, textvariable=self.v_evidencias_catalogo).\
              grid(row=2, column=1, sticky='w')
        Label(lf_dataset_evidencias, text='Robustas:').grid(row=3, column=0,
                                                            sticky='e')
        self.v_evidencias_robustas = StringVar(root, '-------')
        Label(lf_dataset_evidencias, textvariable=self.v_evidencias_robustas).\
              grid(row=3, column=1, sticky='w')
        
        #Dataset de clasificación / Atributos
        lf_dataset_clase_y_atributos = LabelFrame(lf_dataset,
                                       text='Clase y atributos')
        lf_dataset_clase_y_atributos.grid(row=3, column=1, sticky='nsew',
                                          columnspan=3, padx=5, pady=5)
        
        PROPIEDADES_ATRIBUTOS = ('count', 'unique', 'top', 'freq', 'mean',
                                 'std', 'min', '25%', '50%', '75%', 'max')

        self.sb_h_tv_clase = Scrollbar(lf_dataset_clase_y_atributos,
                                       orient='horizontal')
        self.sb_h_tv_clase.grid(row=1, column=0, sticky='ew')
        self.tv_clase = Treeview(lf_dataset_clase_y_atributos,
                                 columns=PROPIEDADES_ATRIBUTOS,
                                 height=1,
                                 xscrollcommand=self.sb_h_tv_clase.set)
        self.tv_clase.grid(row=0, column=0, sticky='ew')
        self.sb_h_tv_clase.config(command=self.tv_clase.xview)
        self.tv_clase.heading("#0", text="Nombre")
        self.tv_clase.column("#0", minwidth=50, width=100, stretch=False)

        self.sb_v_tv_atributos = Scrollbar(lf_dataset_clase_y_atributos)
        self.sb_v_tv_atributos.grid(row=2, column=1, sticky='sn')
        self.sb_h_tv_atributos = Scrollbar(lf_dataset_clase_y_atributos,
                                           orient='horizontal')
        self.sb_h_tv_atributos.grid(row=3, column=0, sticky='ew')
        self.tv_atributos = Treeview(lf_dataset_clase_y_atributos,
                                     columns=PROPIEDADES_ATRIBUTOS,
                                     yscrollcommand=self.sb_v_tv_atributos.set,
                                     xscrollcommand=self.sb_h_tv_atributos.set)
        self.tv_atributos.grid(row=2, column=0, sticky='nsew')
        self.tv_atributos.bind('<ButtonRelease-1>', self.selectItem)
        self.sb_v_tv_atributos.config(command=self.tv_atributos.yview)
        self.sb_h_tv_atributos.config(command=self.tv_atributos.xview)
        self.tv_atributos.heading("#0", text="Nombre")
        self.tv_atributos.column("#0", minwidth=50, width=100, stretch=False)        
        
        for i in PROPIEDADES_ATRIBUTOS:
            self.tv_clase.heading(i, text=i)
            self.tv_clase.column(i, minwidth=50, width=50, stretch=False)
            self.tv_atributos.heading(i, text=i)
            self.tv_atributos.column(i, minwidth=50, width=50, stretch=False)

        lf_dataset_clase_y_atributos.rowconfigure(2, weight=1)
        lf_dataset_clase_y_atributos.columnconfigure(0, weight=1)

        lf_dataset.rowconfigure(3, weight=1)


    def abrir_dataset(self):
        inicio = time.time()
        
        #TODO Las constantes se podrán modificar a través de .cfg
        nombre = askopenfilename(initialdir=self._ruta_datasets,
                         filetypes =(('Archivos de valores separado por comas',
                                      '*.csv'),
                                     ('Todos los archivos', '*.*')),
                         title = 'Selecciona un Dataset de Clasificación')

        self.root.focus_force()
                        
        if not nombre:
            return
        
        self.v_nombre_dataset.set(os.path.splitext(os.path.basename(nombre))[0])
        #TODO Esto debería hacerlo en otro sitio, no cuando lo elijo con
        #     filedialog, y tener en cuenta self._usa_sha1
        if os.path.exists(os.path.dirname(nombre)):
            self.l_nombre_dataset.configure(style='G.TLabel')
        else:
            self.l_nombre_dataset.configure(style='R.TLabel')
        self.v_tamanyo_dataset.set(tamanyo_legible(os.path.getsize(nombre)))
        
        self.v_ruta_dataset.set(os.path.relpath(os.path.dirname(nombre)) \
                                if self._rutas_relativas.get() else \
                                os.path.dirname(nombre))
        self.limpia_datos()
        
        self.root.update()

        #TODO Usar hilos o procesos para leer grandes datasets sin problemas
#        self.progreso = Toplevel(self.root)
#        self.progreso.title("Leyendo Dataset de Clasificación")
#        barra = Progressbar(self.progreso, length=200, mode="indeterminate")
#        barra.pack()
#        self.q = Queue()
#        hilo_lectura = Process(target=self.get_dc)
#        hilo_lectura.start()
#        self.dc = self.q.get()
#        hilo_lectura.join()
#        self.progreso.after(1000, self.check_if_running, hilo_lectura, self.progreso)
        self.dc = DC(self.v_ruta_dataset.get(),
                     self.v_nombre_dataset.get(),
                     self._ruta_resultados,
                     guardar_resultados=False,
                     clase_al_final=self._clase_al_final,
                     mostrar_proceso=False,
                     num_filas_a_leer=None,
                     obtener_catalogo_robusto=False,
                     guardar_datos_proyecto=False,
                     mostrar_uso_ram=False)

        self.escribe_muestra()

        self.v_evidencias_total.set('{:,}'.\
                          format(self.dc.info_dataset_original.num_evidencias))
        self.v_evidencias_completas.set('{:,}'.\
            format(self.dc.info_dataset_sin_datos_desconocidos.num_evidencias))
        self.v_evidencias_catalogo.set('{:,}'.\
            format(self.dc.info_catalogo.num_evidencias))
        self.v_evidencias_robustas.set('{:,}'.\
            format(self.dc.info_catalogo_robusto.num_evidencias))
        
        #TODO Mover a función.
        df = self.dc.info_dataset_original.info_atributos

        self.tv_clase["displaycolumns"] = list(df.index)
        self.tv_atributos["displaycolumns"] = list(df.index)

        for i in df.index:
            self.tv_clase.heading(i, text=i)
            self.tv_clase.column(i, minwidth=50, width=50, stretch=False)
            self.tv_atributos.heading(i, text=i)
            self.tv_atributos.column(i, minwidth=50, width=50, stretch=False)

#        for i in self.tv_clase.get_children():
#            self.tv_clase.delete(i)
#        for i in self.tv_atributos.get_children():
#            self.tv_atributos.delete(i)

        #TODO En australian, aunque guarda bien los valores no los muestra todos.
        for atributo in df.columns:
            valores = [valor if not pd.isnull(valor) else '-' for valor \
                       in df[atributo]]
#            valores = [valor for valor in df[atributo]]
            if atributo == self.dc.info_dataset_original.clase:
                self.tv_clase.insert('', 'end', text=(atributo),values=valores)
            else:
                self.tv_atributos.insert('', 'end', text=(atributo),
                                         values=valores)

        self.root.title('{} - {}'.format(APP_NAME,self.v_nombre_dataset.get()))


#    def get_dc(self):
#        #TODO No puedo crear self.dc en un try mientras depure DC
##        try:
#        dc = DC(self.v_ruta_dataset.get(),
#                     self.v_nombre_dataset.get(),
#                     self._ruta_resultados,
#                     guardar_resultados=False,
#                     clase_al_final=self._clase_al_final,
#                     mostrar_proceso=False,
#                     num_filas_a_leer=None,
#                     obtener_catalogo_robusto=False,
#                     guardar_datos_proyecto=False,
#                     mostrar_uso_ram=False)
#        self.q.put(dc)
##        except Exception as e:
##            self.t_muestra.delete(1.0, 'end')
##            self.t_muestra.insert('end', e)


    def check_if_running(self, hilo, ventana):
        """Check every second if the function is finished."""
        if hilo.is_alive():
            ventana.after(1000, self.check_if_running, hilo, ventana)
        else:
            ventana.destroy()


    def abrir_proyecto(self):
        inicio = time.time()
        
        #TODO Diseñar estrategia para que el usuario sepa si ha de cambiarlo.
        #     Podría bastar con mostrarle los atributos y sus características.
#        clase_al_final = self._clase_al_final
        
        #TODO Las constantes se podrán modificar a través de .cfg
        nombre = askopenfilename(initialdir=self._ruta_resultados,
                                 filetypes =(('Proyectos ACDC', '*.prjACDC'),
                                             ('Todos los archivos', '*.*')),
                                 title = 'Selecciona un proyecto ACDC')
        if nombre is None:
            return
        
        #TODO ¿Mostrar sólo el nombre del dataset original?
        self.root.title('{} - {}'.format(APP_NAME,
                        os.path.splitext(os.path.basename(nombre))[0]))


    def rutas(self, r=None):
        ruta = askdirectory(title='Directorio de {}'.format(r),
                            initialdir=eval('self._ruta_{}'.format(r)),
                            mustexist=True)
        if ruta == '':
            print(None)
        else:
            if self._rutas_relativas.get():
                #TODO Sólo sirve en UNIX y macOS ¿Qué pasa con MS-Windows?
                print(os.path.relpath(ruta))
                print(APP_DIR)
            else:
                print(ruta)


    def limpia_datos(self):
        self.t_muestra['state'] = 'normal'
        self.t_muestra.delete(1.0, 'end')
        self.t_muestra['state'] = 'disabled'
        for i in self.tv_clase.get_children():
            self.tv_clase.delete(i)
        for i in self.tv_atributos.get_children():
            self.tv_atributos.delete(i)

    def tamanyo_muestra(self, tamanyo):
        nuevo_tamanyo = askinteger('Muestra',
                                   '¿Cuántas evidencias quieres ver?',
                                   parent=self.root, minvalue=1,
                                   initialvalue=self._tamanyo_muestra)
#                                            minvalue=0, maxvalue=1000)
        if nuevo_tamanyo:
            self._tamanyo_muestra = nuevo_tamanyo
        #TODO Debería averiguar el índice del menú que quiero modificar
        self.m_configuracion.entryconfigure(4, label='Tamaño muestra ({:,})'.\
                                                 format(self._tamanyo_muestra))
        self.escribe_muestra()


    #TODO Tratar excepciones para que no se quede habilitada la escritura
    def escribe_muestra(self):
        self.t_muestra['state'] = 'normal'
#        self.t_muestra.delete(1.0, 'end')
#        self.t_muestra.insert('end', tiempo_transcurrido(time.time() - inicio) \
#                              + ' leyendo catálogo\n')
#        self.t_muestra.insert('end', '\n')
        self.t_muestra.insert('end', '#######################################'\
                              '########################\n')
        self.t_muestra.insert('end', '   PRIMERAS {:,} LÍNEAS DEL DATASET DE '\
                              'CLASIFICACIÓN ORIGINAL\n'.\
                              format(self._tamanyo_muestra))
        self.t_muestra.insert('end', '#######################################'\
                              '########################\n')
        self.t_muestra.insert('end', 
                              self.dc.muestra(self._tamanyo_muestra))
        
#        self.t_muestra.insert('end', '\n\n#############################\n')
#        self.t_muestra.insert('end', 'DESCRIPCIÓN ATRIBUTOS Y CLASE\n')
#        self.t_muestra.insert('end', '#############################\n')
#        self.t_muestra.insert('end', self.dc.describe_atributos_y_clase())
#        atributos_y_clase = self.dc.describe_atributos_y_clase()
#        print(atributos_y_clase)
#        print()
#        print(atributos_y_clase.info())
#        print()
#        print('count')
#        print(atributos_y_clase.loc['count'])
#        print()
#        print('count')
#        total = self.dc.num_evidencias_dataset
#        print(type(atributos_y_clase.loc['count']))
#        for i, c in enumerate(atributos_y_clase.loc['count']):
#            if c < total:
#                print(type(c))
#                print('¡La columna {} tiene {} valores!'.format(i, c))
#                print('¡El atributo {} tiene {} valores!'.format(atributos_y_clase.columns[i], c))
#            
#        print()
#        print(type(atributos_y_clase))
#        print()
#        for i in atributos_y_clase:
#            print(i)
#            print(type(i))
        
        
#        print()
#        print('INFO_DATASET_ORIGINAL\n')
#        print(self.dc.info_dataset_original.__dict__)
##        for i, j in enumerate(self.dc.info_dataset_original.__dict__):
##            print('\t{} {}'.format(j, self.dc.info_dataset_original.__dict__[i]))
#        print()
#        for i in self.dc.info_dataset_original.__dict__:
##            print('\t{}'.format(i))
#            print('\t{}: {}'.format(i, self.dc.info_dataset_original.__dict__[i]))
#        print()
#        print('\tUSO_MEMORIA: {}'.format(tamanyo_legible(self.dc.info_dataset_original.__dict__['uso_memoria'])))
#        
        print()
        print('INFO')
        print(self.dc.dataset_info())
        
        print()
        print('Atributos con DD')
        print(self.dc.atributos_con_datos_desconocidos())
        print()
        
        self.t_muestra.insert('end', '\n\n')
        self.t_muestra['state'] = 'disabled'


    #TODO Modificar para que muestre información relevante de la celda.
    def selectItem(self, event):
        curItem = self.tv_atributos.item(self.tv_atributos.focus())
        col = self.tv_atributos.identify_column(event.x)
        print ('curItem = ', curItem)
        print ('col = ', col)
    
        if col == '#0':
            cell_value = curItem['text']
        else:
            cell_value = curItem['values'][int(col[1:])-1]
#        elif col == '#1':
#            cell_value = curItem['values'][0]
#        elif col == '#2':
#            cell_value = curItem['values'][1]
#        elif col == '#3':
#            cell_value = curItem['values'][2]
        print ('cell_value = ', cell_value)


    def guarda_configuracion(self):
        #Dimensiones y posición de la ventana
        self.root.update()
        ancho, alto = self.root.winfo_width(), self.root.winfo_height()
        x, y = self.root.winfo_x(), self.root.winfo_y()
        
        print('Dimensiones y posición al cerrar: {}x{}+{}+{}'.format(ancho, 
              alto, x, y))


    def cerrar_aplicacion(self):
        self.guarda_configuracion()
        self.root.destroy()



if __name__ == '__main__':
    root = Tk()
    GUIDatasetClasificacion(root)
    root.mainloop()
