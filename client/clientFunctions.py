import time
import socket
import os
import json  # Biblioteca para manipulação de JSON

file_path = "/shared/output.txt"

def receive_data(client):
    # Recebe os dados em formato JSON e converte para um dicionário
    try:
        data = client.recv(1024).decode('utf-8')
        return json.loads(data)  # Retorna o dicionário após carregar o JSON
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return {}  # Retorna um dicionário vazio em caso de erro

def get_current_timestamp():
    return float(time.time())
