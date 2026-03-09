import os
import gc
import logging
import geopandas as gpd

from pathlib import Path


def zips_to_parquet(input_folder: str, output_path: str, output_foldername: str):
    """
    Lê arquivos ZIP contendo shapefiles, padroniza o CRS (EPSG:4674) e salva 
    cada um como um arquivo GeoParquet independente dentro de um diretório (Dataset).
    """
    input_dir = Path(input_folder)
    
    final_output_dir = Path(output_path) / output_foldername
    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Busca todos os arquivos .zip na pasta
    zip_files = list(input_dir.glob("*.zip"))
    
    if not zip_files:
        logging.error("Nenhum arquivo .zip encontrado na pasta de entrada.")
        raise FileNotFoundError(f"Nenhum arquivo .zip encontrado em: {input_folder}")
    
    # 2. Processar e salvar CADA arquivo zip individualmente
    logging.info(f"Iniciando processamento de {len(zip_files)} arquivos. O dataset será salvo em: {final_output_dir}")
    
    for zip_path in zip_files:
        try:
            # Leitura do arquivo zip via pyogrio (alta performance)
            temp_gdf = gpd.read_file(f"zip://{zip_path}", engine='pyogrio')
            
            # Verificação e padronização do sistema de coordenadas (SIRGAS 2000)
            if temp_gdf.crs is None:
                temp_gdf.set_crs(epsg=4674, inplace=True)
            elif temp_gdf.crs.to_epsg() != 4674:
                temp_gdf = temp_gdf.to_crs(epsg=4674)

            # Define o nome do arquivo parquet com base no nome do zip (Ex: GO.zip -> GO.parquet)
            parquet_filename = zip_path.with_suffix('.parquet').name
            parquet_filepath = final_output_dir / parquet_filename
            
            # Salvar DIRETAMENTE no disco, sem acumular em listas
            temp_gdf.to_parquet(parquet_filepath, write_covering_bbox=True)
            logging.info(f"✅ Sucesso: {zip_path.name} convertido para {parquet_filename}")
            
            # 3. Limpeza agressiva de memória RAM
            del temp_gdf
            gc.collect()  # Força o Python a liberar a memória imediatamente
            
        except Exception as e:
            logging.error(f"❌ Erro ao processar {zip_path.name}: {e}")

    logging.info(f"🚀 Pipeline finalizado! Dataset particionado criado em: {final_output_dir}")


def zips_to_parquet(input_folder: str, output_path: str, output_foldername: str):
    """
    Lê arquivos ZIP contendo shapefiles, padroniza o CRS (EPSG:4674), 
    adiciona colunas de bounding box para otimização no DuckDB e salva 
    cada um como um arquivo GeoParquet independente.
    """
    input_dir = Path(input_folder)
    
    final_output_dir = Path(output_path) / output_foldername
    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    zip_files = list(input_dir.glob("*.zip"))
    
    if not zip_files:
        logging.error("Nenhum arquivo .zip encontrado na pasta de entrada.")
        raise FileNotFoundError(f"Nenhum arquivo .zip encontrado em: {input_folder}")
    
    logging.info(f"Iniciando processamento de {len(zip_files)} arquivos. O dataset será salvo em: {final_output_dir}")
    
    for zip_path in zip_files:
        try:
            temp_gdf = gpd.read_file(f"zip://{zip_path}", engine='pyogrio')
            
            if temp_gdf.crs is None:
                temp_gdf.set_crs(epsg=4674, inplace=True)
            elif temp_gdf.crs.to_epsg() != 4674:
                temp_gdf = temp_gdf.to_crs(epsg=4674)

            bounds = temp_gdf.geometry.bounds
            temp_gdf['min_x'] = bounds['minx']
            temp_gdf['min_y'] = bounds['miny']
            temp_gdf['max_x'] = bounds['maxx']
            temp_gdf['max_y'] = bounds['maxy']

            parquet_filename = zip_path.with_suffix('.parquet').name
            parquet_filepath = final_output_dir / parquet_filename
            
            temp_gdf.to_parquet(parquet_filepath, write_covering_bbox=True)
            logging.info(f"✅ Sucesso: {zip_path.name} convertido para {parquet_filename}")
            
            del temp_gdf
            gc.collect() 
            
        except Exception as e:
            logging.error(f"❌ Erro ao processar {zip_path.name}: {e}")

    logging.info(f"🚀 Pipeline finalizado! Dataset particionado criado em: {final_output_dir}")



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True  # Garante que esta configuração prevaleça
    )

    input_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(input_path, exist_ok=True)
    
    zips_to_parquet(input_path, input_path, 'car-br-dataset')
