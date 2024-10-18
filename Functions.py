import time
import re
import socket
import threading
import random
import os
import json  # Biblioteca para manipulação de JSON

file_path = "/shared/output.txt"


def get_current_timestamp():
    return float(time.time())

def create_containers(elements):
    return [{'id': i + 1, 'cluster_port': 6000 + i + 1, 'timestamp': -2, 'responded': 'no'} for i in range(elements)]

def receive_data(client):
    # Recebe os dados em formato JSON e converte para um dicionário
    try:
        data = client.recv(1024).decode('utf-8')
        return json.loads(data)  # Retorna o dicionário após carregar o JSON
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return {}

def send_data(client_socket, message):
    try:
        if not client_socket._closed:
            # Envia os dados no formato JSON
            client_socket.send(json.dumps(message).encode('utf-8'))
        else:
            print("Tentativa de enviar dados em um socket fechado.")
    except ConnectionRefusedError:
        print(f"Falha ao conectar no container {client_socket['id']}")
        return -2  # Retorna -2 para indicar falha na conexão
    except OSError as e:
        print(f"Erro ao enviar dados: {e}")    

# Função para extrair a mensagem de um dicionário JSON
def extract_message(data):
    return data.get("message", -1)

def extract_id(data):
    return data.get("client_id", -1)

def extract_timestamp(data):
    return data.get("timestamp", -1.0)

def create_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Servidor aguardando conexões na porta {port}...")
    return server_socket

def accept_client(server_socket):
    client_socket, addr = server_socket.accept()
    print(f"Conexão estabelecida com {addr}")
    return client_socket

def received_timestamps(containers):
    for con in containers:
        if con['timestamp'] == -2:
            return False
    return True

def received_oks(containers):
    for con in containers:
        if con['start'] != 'OK':
            return False
    return True

# Função de comparação de timestamps
def compare_by_timestamp(container):
    return container['timestamp']

def beautifull_print(containers):
    for con in containers:
        print(con)

#--------------------- TEST AREA
