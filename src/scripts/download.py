import os

from SICAR import Sicar, State, Polygon


def download(output_path: str):
    for itter in range(1, 4):
        <verificar quais estados foram baixados: a partir da pasta output_path verificar os nomes dos arquivos ex: "GO_AREA_IMOVEL.zip">
        sicar = Sicar()
    
        states = State

        for missing_states:
            print("Inicializando download dos CARs...")
            sicar.download_state(polygon=Polygon.AREA_PROPERTY, folder=output_path, tries=50)
   
if __name__ == "__main__":
    output_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(output_path, exist_ok=True)

    download(output_path)