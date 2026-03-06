import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler

# Importando as funções otimizadas dos seus módulos
from src.scripts.download import download
from src.scripts.transformations import zips_to_parquet

# 1. Configuração do Log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logger = logging.getLogger(__name__)


FUSO_BR = ZoneInfo("America/Sao_Paulo")


# Passamos o scheduler como argumento para que a task possa criar novos agendamentos (retrys)
def task(scheduler):
    logger.info("Iniciando tarefa de atualização do CAR...")

    # 2. Cria um diretório temporário seguro
    with tempfile.TemporaryDirectory() as tmp_folder:
        logger.info(f"Diretório temporário criado em: {tmp_folder}")

        try:
            # 3. Executa o download dos estados
            logger.info("Baixando dados do SICAR...")
            download(tmp_folder)
            
            # 4. Transforma os zips no GeoParquet Dataset
            output_path = Path.cwd() / 'data'
            
            logger.info("Convertendo arquivos para GeoParquet Dataset...")
            zips_to_parquet(
                input_folder=tmp_folder, 
                output_path=str(output_path), 
                output_foldername='car-br-dataset'
            )
            
            logger.info("✅ Tarefa concluída com sucesso! O diretório temporário será limpo automaticamente.")
            
        except Exception as e:
            # 5. O logger.exception captura a linha exata do erro e todo o rastro (traceback)
            logger.exception(f"❌ Erro fatal durante a execução da tarefa: {e}")
            
            # ----------------------------------------------------------------
            # LÓGICA DE RETRY: Agenda para o dia seguinte em caso de erro
            # ----------------------------------------------------------------
            now = datetime.now(FUSO_BR)
            # Adiciona 1 dia (24 horas)
            tomorrow = now + timedelta(days=1)
            # Padroniza para rodar sempre à 00:01 do dia seguinte
            proxima_tentativa = tomorrow.replace(hour=0, minute=1, second=0, microsecond=0)
            
            logger.warning(f"🔄 Reagendando nova tentativa para: {proxima_tentativa}")
            
            # Cria um job de execução única para o dia seguinte.
            # Se esse job falhar amanhã, ele cairá neste except de novo e jogará para depois de amanhã.
            scheduler.add_job(
                task, 
                'date', 
                run_date=proxima_tentativa, 
                args=[scheduler], 
                id='retry_task', 
                replace_existing=True # Sobrescreve caso já exista um retry pendente
            )


if __name__ == "__main__":
    # 6. Definindo o fuso horário para garantir a execução correta, independente do servidor
    scheduler = BlockingScheduler(timezone="America/Sao_Paulo")
    
    # LÓGICA MENSAL: Agendado para o dia 1 de cada mês à 00:01
    # O id='main_monthly_task' garante que este job nunca seja duplicado
    scheduler.add_job(
        task, 
        'cron', 
        day=1, 
        hour=0, 
        minute=1, 
        args=[scheduler], 
        id='main_monthly_task'
    )
    
    # ----------------------------------------------------------------
    # LÓGICA DE STARTUP: Executa imediatamente assim que o código sobe
    # ----------------------------------------------------------------
    logger.info("🚀 Agendando execução imediata de inicialização...")
    scheduler.add_job(
        task, 
        'date', 
        args=[scheduler], 
        id='startup_task'
    )
    
    logger.info("🕒 Agendador iniciado. Aguardando execuções...")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Agendador interrompido pelo usuário.")