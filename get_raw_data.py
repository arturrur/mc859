import requests
import time
import pandas as pd
import json


url_deputados = "https://dadosabertos.camara.leg.br/api/v2/deputados"

class CamaraAPIGet:
    '''
    Classe para fazer chamadas GET à API de Dados Abertos da Câmara dos Deputados
    '''
    def __init__(self):
        self.base_url = "https://dadosabertos.camara.leg.br/api/v2/"
    
    
    # função privada responsável por percorrer todas as páginas da API e retornar todos os itens encontrados 
    def _get_all_data(self, endpoint, parameters):
        complete_data = []
        curr_url = f"{self.base_url}/{endpoint}"
        
        while curr_url:
            try:
                response = requests.get(curr_url, params=parameters, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data['dados']:
                    complete_data.extend(data['dados'])
                    print(f"Mais {len(data['dados'])} coletados, total até agora: {len(complete_data)}.")
                    
                
                # Pega a url da próxima página, se existir.
                curr_url = None
                for link in data['links']:
                    if link['rel'] == 'next':
                        curr_url = link['href']
                
                # Remove os valores em parâmetros, pois já estão dentro do curr_url
                parameters = None
                time.sleep(1)
            
            except requests.exceptions.RequestException as e:
                print(f"Ocorreu um erro ao acessar {curr_url}: {e} ")
                break
            
        print(f"Terminada a coleta do endpoint {endpoint}. Número de itens coletados: {len(complete_data)}.")
        return complete_data

    def get_deputados(self, id_legislatura):
        '''
        Retorna todos os deputados da legislatura especificada.
        '''
        endpoint = "deputados"
        parameters = {
            'idLegislatura': id_legislatura,
            'itens': 100
        }
        return self._get_all_data(endpoint, parameters)
    
    def get_votacoes(self, lista_anos):
        '''
        Retorna todas as votações ocorridas nos anos especificados.
        '''
        endpoint = 'votacoes'
        data = []
        for ano in lista_anos:
            parameters = {
                'dataInicio': f'{ano}-01-01',
                'itens': 100
            }
            data.extend(self._get_all_data(endpoint, parameters))
        
        
    


# parameters = {
#     'idLegislatura': 56,
#     'itens': 100
# }

# response = requests.get(url_deputados, params=parameters, timeout=30)

# data = response.json()

# lista_deputados = data['dados']
# next = data['links'][1]['href']

# df_deputados = pd.DataFrame(lista_deputados)

# response2 = requests.get(next, timeout=30)

# data = response2.json()
# lista = data['links']

# df_deputados2 = pd.DataFrame(lista)

# print(df_deputados)
# print(df_deputados2)

if __name__ == "__main__":
    client = CamaraAPIGet()
    
    deputados = client.get_deputados(id_legislatura=56)
    
    
    with open('deputados_raw.json', 'w') as f:
        json.dump(deputados, f, ensure_ascii=False, indent=4)
        
    test = pd.DataFrame(deputados)
    print(len(test['id'].unique()))
    print(test)
    
