import os
import tempfile

from apscheduler.schedulers.blocking import BlockingScheduler

from src.scripts.download import download
from src.scripts.transformations import zips_to_parquet

def task():
    print("Iniciando tarefa mensal de atualização do CAR...", flush=True)

    # 1. Cria um diretório temporário seguro
    with tempfile.TemporaryDirectory() as tmp_folder:
        print(f"Diretório temporário criado em: {tmp_folder}", flush=True)

        try:
            # 2. Executa o download (Salvando no diretório temporário)
            print("Baixando dados do SICAR...", flush=True)
            download(tmp_folder)
            
            # 3. Transforma os zips em um único (ou vários) GeoParquet
            output_path = os.path.join(os.getcwd(), 'data')
            os.makedirs(output_path, exist_ok=True)
            
            print("Convertendo arquivos para GeoParquet...", flush=True)
            zips_to_parquet(tmp_folder, output_path)
            
            print("Tarefa concluída com sucesso! Limpando arquivos temporários...", flush=True)
            
        except Exception as e:
            print(f"Erro durante a execução: {e}", flush=True)


scheduler = BlockingScheduler()

scheduler.add_job(task, 'cron', day=1, hour=0, minute=1)

print("Agendador iniciado...", flush=True)
scheduler.start()