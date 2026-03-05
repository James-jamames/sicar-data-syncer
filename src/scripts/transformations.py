import os
import logging
import pandas as pd
import geopandas as gpd

from pathlib import Path


def zips_to_parquet(input_folder: str, output_path: str, output_filename: str):
    """
    Lê arquivos ZIP contendo shapefiles, padroniza o CRS (EPSG:4674) e consolida em um arquivo GeoParquet.
    """
    input_dir = Path(input_folder)
    output_dir = Path(output_path)
    
    # 1. Busca todos os arquivos .zip na pasta
    zip_files = list(input_dir.glob("*.zip"))
    
    if not zip_files:
        logging.error("Nenhum arquivo .zip encontrado na pasta de entrada.")
        raise FileNotFoundError(f"Nenhum arquivo .zip encontrado em: {input_folder}")
    
    gdf_list = []
    
    # 2. Inserir cada arquivo zip em um DataFrame geográfico
    logging.info(f"Iniciando processamento de {len(zip_files)} arquivos...")
    
    for zip_path in zip_files:
        try:
            # Leitura do arquivo zip via pyogrio (alta performance)
            temp_gdf = gpd.read_file(f"zip://{zip_path}", engine='pyogrio')
            
            # Verificação e padronização do sistema de coordenadas (SIRGAS 2000)
            if temp_gdf.crs is None:
                temp_gdf.set_crs(epsg=4674, inplace=True)
            elif temp_gdf.crs.to_epsg() != 4674:
                temp_gdf = temp_gdf.to_crs(epsg=4674)

            gdf_list.append(temp_gdf)
            logging.info(f"✅ Sucesso: {zip_path.name}")
            
        except Exception as e:
            logging.error(f"❌ Erro ao processar {zip_path.name}: {e}")

    # 3. Concatenar arquivos do CAR
    if not gdf_list:
        logging.error("Nenhum dado foi processado.")
        raise ValueError("A lista de GeoDataFrames está vazia.")
        
    logging.info("Consolidando dados...")

    # Arquivo único com todos os imóveis rurais
    final_gdf = pd.concat(gdf_list, ignore_index=True)
    
    # Garantir que o resultado não perdeu a classe GeoDataFrame após o concat
    if not isinstance(final_gdf, gpd.GeoDataFrame):
        final_gdf = gpd.GeoDataFrame(final_gdf, geometry='geometry', crs="EPSG:4674")
            
    # 4. Criar diretório de saída e salvar no formato GeoParquet
    output_dir.mkdir(parents=True, exist_ok=True)
    final_filepath = output_dir / output_filename
    
    logging.info("Salvando arquivo GeoParquet...")
    final_gdf.to_parquet(final_filepath)
    
    logging.info(f"🚀 Pipeline finalizado! Arquivo salvo em: {final_filepath}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True  # Garante que esta configuração prevaleça
    )

    input_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(input_path, exist_ok=True)
    
    zips_to_parquet(input_path, input_path, 'car-br.parquet')
