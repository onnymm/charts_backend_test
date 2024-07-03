import os
import re

def define_local_origin() -> str:

    # Ejecución del comando en terminal
    os.system("ipconfig > temp_file")

    # Extracción de la dirección IP local
    ip = ""
    with open("temp_file", 'r') as file:
        for line in file:
            find = re.search(r"IPv4.*192\.168\.\d{1,3}\.\d{1,3}", str(line))
            
            if find:
                start, end = find.span()
                line = line[start:end]
                start, end = re.search(r"192\.168\.\d{1,3}\.\d{1,3}", line).span()
                ip = line[start:end]
                break
    
    # Se elimina el archivo
    os.system("rm temp_file")

    return ip