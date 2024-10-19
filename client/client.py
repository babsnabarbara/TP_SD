import socket
import time
import random
import json
from clientFunctions import *

def main():
    quer_escrever = 1

    port = int(os.getenv('PORT'))
    client_id = int(os.getenv('ID'))
    host = f"container_{client_id}"

    commited = False
    
    # Cria o socket do cliente
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))  # Conectar ao servidor

    try:
        i = 0
        while True:
            
            
            # Simulação de cliente querendo ou não escrever
            if not commited and quer_escrever != 0:
                timestamp = int(time.time() * 10000)
                commited = True

            # Montar mensagem com JSON
            message = {
                "client_id": client_id,
                "timestamp": timestamp,
                "message": f"{i}"
            }

            if i >= 50:
                message = {
                    "client_id": client_id,
                    "timestamp": timestamp,
                    "message": ""
                }
            
            if quer_escrever == 0:
                message['message'] = -1
            # Serializa a mensagem em JSON e envia
            client_socket.send(json.dumps(message).encode('utf-8'))

            # Receber a resposta do servidor
            cluster_command = receive_data(client_socket)
            cluster_command = json.loads(cluster_command)
            


            # Processa a resposta do servidor
            #print(cluster_command)
            #time.sleep(0.2)
            #if cluster_command == {"status": "sleep"}:  
            #    time.sleep(0.1) 
            if cluster_command == {"status": "committed"}:
                i += 1
                commited = False
                
            
    except OSError as e:
        print(f"Erro ao enviar dados: {e}")
    except KeyboardInterrupt:
        client_socket.close()
    finally:
        client_socket.close()  # Fechar o socket do cliente

if __name__ == "__main__":  
    main()
