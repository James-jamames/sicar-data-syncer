import os
import glob
import pandas as pd
import geopandas as gpd


def zips_to_parquet(input_folder: str, output_path: str, output_filename: str):
    #Busca todos os arquivos .zip na pasta
    zip_files = glob.glob(os.path.join(input_folder, "*.zip"))
    if not zip_files:
        raise Exception("Nenhum arquivo .zip encontrado.")
    
    #Lista para armazenar as camadas
    gdf_list = []

    #Inserir cada arquivo zip em um DataFrame geográfico
    print(f"Iniciando processamento de {len(zip_files)} arquivos...", flush=True)
    for zip_path in zip_files:
        try:
            #Leitura do arquivo zip
            temp_gdf = gpd.read_file(f"zip://{zip_path}", engine='pyogrio')
            
            #Verificação do sistema de coordenadas (ex: SIRGAS 2000)
            if temp_gdf.crs is None:
                temp_gdf.set_crs(epsg=4674, inplace=True)
            else:
                temp_gdf = temp_gdf.to_crs(epsg=4674)

            gdf_list.append(temp_gdf)
            print(f"Sucesso: {os.path.basename(zip_path)}", flush=True)
            
        except Exception as e:
            print(f"Erro ao processar {zip_path}: {e}", flush=True)

    #Concatenar arquivos do CAR
    if gdf_list:
        print("Consolidando dados...")

        #Arquivo único com todos os imóveis rurais do CAR       
        final_gdf = pd.concat(gdf_list, ignore_index=True)
                
        #Salvando no formato Parquet (GeoParquet)
        final_gdf.to_parquet(os.path.join(output_path, output_filename))
        print(f"Pipeline finalizado! Arquivo salvo como: {output_filename}")
    else:
        raise Exception("Nenhum dado foi processado.")

if __name__ == "__main__":
    input_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(input_path, exist_ok=True)
    
    zips_to_parquet(input_path, input_path, 'car-br.parquet')
