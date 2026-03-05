import os
import time
import logging

from SICAR import Sicar, State, Polygon


def download(output_path: str, max_retries: int = 3):
    """
    Baixa os polígonos do CAR para todos os estados usando a biblioteca SICAR.
    """
    states = State
    sicar = Sicar()

    for state in states:
        logging.info(f"Iniciando o download para o estado: {state}")
        
        for attempt in range(1, max_retries + 1):
            try:
                sicar.download_state(
                    state=state, 
                    polygon=Polygon.AREA_PROPERTY, 
                    folder=output_path, 
                    tries=50
                )
                logging.info(f"✅ Download concluído com sucesso para: {state}")
                break  # Sai do loop de tentativas se der certo
                
            except Exception as e:
                wait_time = 5 * (2 ** attempt)
                logging.warning(f"⚠️ Falha no estado {state} (Tentativa {attempt}/{max_retries}). Erro: {e}")
                
                if attempt < max_retries:
                    logging.info(f"Aguardando {wait_time} segundos para tentar novamente...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"❌ Falha definitiva ao baixar {state} após {max_retries} tentativas.")

   
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True  # Garante que esta configuração prevaleça
    )

    output_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(output_path, exist_ok=True)

    download(output_path)