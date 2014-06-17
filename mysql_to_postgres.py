#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from random import choice
import string, sys

def main():
    
    start = datetime.now()
    
    ####################################
    # Base de datos MySQL analizo.info #
    ####################################
    
    sql_file = open('db440577083.db.1and1.com.sql', 'r')
    
    db = {}
    
    ###############################################
    # Diccionario {tabla: registros} analizo.info #
    ###############################################
    
    for line in sql_file:
        
        if 'CREATE TABLE' in line:
            
            table_name = line.split()[5].replace('`', '')
            
            db[table_name] = []
        
        if 'VALUES' in line:
            
            l = db[table_name]
            
            l.append(line.split('VALUES')[1][2:-3] + '\n')
            
            db[table_name] = l
            
        
    
    sql_file.close()
    
    ######################################
    # Generar {tabla: registros} PyBossa #
    ######################################
    
    #####################################################
    # analizo.info "pla_platformusers" - PyBossa "user" #
    #####################################################
    
    n = '\n'
    output_data = ''
    debug_data = ''
    
    header = '--' + n + '-- PostgreSQL database dump' + n + '--' + n + n + 'SET statement_timeout = 0;' + n + "SET client_encoding = 'UTF8';" + n + 'SET standard_conforming_strings = on;' + n + 'SET check_function_bodies = false;' + n + 'SET client_min_messages = warning;' + n + n + 'SET search_path = public, pg_catalog;' + n + n + '--' + n + '-- Data for Name: user; Type: TABLE DATA; Schema: public; Owner: ppybossa' + n + '--' + n + n
    
    table_header = 'COPY "user" (id, created, email_addr, name, fullname, locale, api_key, passwd_hash, admin, category, flags, twitter_user_id, facebook_user_id, google_user_id, ckan_api, info) FROM stdin;'
    
    end = '\.' + n + n + n
        
    footer = '--' + n + '-- Name: user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: ppybossa' + n + '--' + n + n
    
    output_data = header
    output_data += table_header
    output_data += n
    
    #######################################
    # Usuarios administradores de PyBossa #
    #######################################
    
    admin_users = ['1    2013-11-16T17:44:17.650481    correo_1@gmail.com    nick_1    Nombre_a_1 Apellido_a_1 Apellido_a_2    es    0ace6eca-a065-430c-a215-70b592c59cc4    pbkdf2:sha1:1000$b78QDlH1$eearecae1845713cncd54392957f12d3471b014f    t    \N    \N    \N    \N    \N    \N    {}',
    '2    2013-11-16T18:17:28.906528    correo_2@gmail.com    nick_2    Nombre_b_1 Apellido_b_1 Apellido_b_2    es    bdf80495-40aa-4756-ba92-2151af571c8d    pbkdf2:sha1:1000$XXmQNWvl$db3413813fe211ad3b51c70ec4bb066e805c80cc    t    \N    \N    \N    \N    \N    \N    {}']
    
    output_data += n.join(admin_users)
    output_data += n
    
    ###############################
    # Campos tabla "user" PyBossa #
    ###############################
    
    id_ = len(admin_users) + 1 # contemplar los usuarios administradores
    # created
    # email_addr
    # name
    # fullname
    # locale
    # api_key
    # passwd_hash
    # admin
    # category
    # flags
    # twitter_user_id
    # facebook_user_id
    # google_user_id
    # ckan_api
    # info
    
    ####################################
    # api_key (evitar keys duplicadas) #
    ####################################
    
    api_keys = {}
    
    for i in range(len(admin_users)):
        api_key = str(admin_users[i].split('\t')[6])
        if checkApiKey(api_key, api_keys):
            print 'Proceso abortado'
            sys.exit()
        
    
    ########################################
    # Posibles caracteres para una api_key #
    ########################################
    
    api_key_values = {}
    
    for s in string.digits: # 0123456789
        api_key_values[s] = ''
    
    for s in string.ascii_lowercase: # abcdefghijklmnopqrstuvwxyz
        api_key_values[s] = ''
    
    ##########################################
    # passwd_hash (evitar passwd duplicados) #
    ##########################################
    
    passwd_hashes = {}
    
    for i in range(len(admin_users)):
        passwd_hash = str(admin_users[i].split('\t')[7])
        if checkPasswdHash(passwd_hash, passwd_hashes):
            print 'Proceso abortado'
            sys.exit()
        
    
    ###########################################
    # Posibles caracteres para un passwd_hash #
    ###########################################
    
    passwd_hash_values = {}
    
    for s in string.digits: # 0123456789
        passwd_hash_values[s] = ''
    
    for s in string.ascii_letters: # abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
        passwd_hash_values[s] = ''
    
    #############################################################
    # Generar equivalencias entre tablas analizo.info - PyBossa #
    #############################################################
     
    for table in db:
        
        if table == 'pla_platformusers': # Tabla a migrar
            
            debug_data += '[*] ' + table + n + n
            
            values = db[table]
            
            for row in values:
                
                debug_data += row
                
                row = row.replace('\'', '')
                
                row = row.split(', ')
                
                output_data += append(str(id_)) # id
                
                id_ += 1
                
                created = row[9].split()
                
                output_data += append(created[0] + 'T' + created[1] + '.000000') # created
                
                output_data += append(row[2]) # email_addr
                
                output_data += append(row[1]) # name
                
                output_data += append(row[1].upper()) # fullname
                
                output_data += append('es') # locale
                
                output_data += append(generateApiKey(api_keys, api_key_values.keys())) # api_key
                
                output_data += append(generatePasswdHash(passwd_hashes, passwd_hash_values.keys(), row[3])) # passwd_hash
                
                output_data += append('f') # admin
                
                output_data += append('\N') # category
                
                output_data += append('\N') # flags
                
                output_data += append('\N') # twitter_user_id
                
                output_data += append('\N') # facebook_user_id
                
                output_data += append('\N') # google_user_id
                
                output_data += append('\N') # ckan_api
                
                output_data += '{}' # info
                
                output_data += n
                
            
        
    
    output_data += end
    
    output_data += footer
    
    output_data += "SELECT pg_catalog.setval('user_id_seq'," + str(id_-1) + ', true);' + n + n
    
    output_data += '--' + n + '-- PostgreSQL database dump complete' + n + '--' + n + n
    
    writeFile('pybossa_user_data_pla_platformusers', output_data, '.sql')
    
    writeFile('debug', debug_data)
    
    print '\n', datetime.now() - start, '\n'

def generatePasswdHash(passwd_hashes, passwd_hash_values, passwd):
    # pbkdf2:sha1:1000$9pkDgYNx$11077cd1efe8544cda235a89a016449a01e9ea08
    # pbkdf2:sha1:1000 - $ 8 $- 40
    
    start, sep = 'pbkdf2:sha1:1000', '$'
    
    ok = True
    
    while ok:
        
        ph, passwd_hash = '', ''
        
        for i in range(0, 8): # 8
            ph += choice(passwd_hash_values)
        
        passwd_hash = start + sep + ph + sep + passwd
        
        ok = checkPasswdHash(passwd_hash, passwd_hashes)
        
    
    return passwd_hash

def checkPasswdHash(passwd_hash, passwd_hashes):
    if passwd_hash in passwd_hashes:
        print 'Aviso: se ha generado un passwd_hash duplicado', passwd_hash
        print 'Generando un nuevo passwd_hash...'
        return True
    else:
        passwd_hashes[passwd_hash] = ''
        return False
    

def generateApiKey(api_keys, api_key_values):
    # 0ace6eca-cf7a-410c-a215-64883cde3154
    # 8 - 4 - 4 - 4 - 12
    
    sep = '-'
    
    ok = True
    
    while ok:
        
        ak1, ak2, ak3, ak4, ak5, api_key = '', '', '', '', '', ''
        
        for i in range(0, 8): # 8
            ak1 += choice(api_key_values)
        
        for i in range(0, 4): # 4
            ak2 += choice(api_key_values)
            
        for i in range(0, 4): # 4
            ak3 += choice(api_key_values)
        
        for i in range(0, 4): # 4
            ak4 += choice(api_key_values)
        
        for i in range(0, 12): # 12
            ak5 += choice(api_key_values)
        
        api_key = ak1 + sep + ak2 + sep + ak3 + sep + ak4 + sep + ak5
        
        ok = checkApiKey(api_key, api_keys)
        
    
    return api_key

def checkApiKey(api_key, api_keys):
    if api_key in api_keys:
        print 'Aviso: se ha generado una api_key duplicada', api_key
        print 'Generando una nueva api_key...'
        return True
    else:
        api_keys[api_key] = ''
        return False
    

def append(value):
    return value + '\t'

def writeFile(file_name, data, ext = '.txt'):
    f = open(file_name + ext, 'w')
    f.write(data)
    f.close()

if __name__ == '__main__':
    main()
