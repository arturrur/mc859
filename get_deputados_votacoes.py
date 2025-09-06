import requests
import time
import pandas as pd
import json


class CamaraAPIGet:
    '''
    Classe para fazer chamadas GET à API de Dados Abertos da Câmara dos Deputados
    '''
    def __init__(self):
        self.base_url = "https://dadosabertos.camara.leg.br/api/v2"
        self.max_tries = 10
    
    
    # função privada responsável por percorrer todas as páginas da API e retornar todos os itens encontrados 
    def _get_all_data(self, endpoint, parameters):
        complete_data = []
        curr_url = f"{self.base_url}/{endpoint}"
        
        while curr_url:
            for attempt in range(self.max_tries):
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
                    
                    # Se deu certo não precisa tentar novamente, vai para a próxima url
                    break
                
                except requests.exceptions.RequestException as e:
                    print(f"-> Erro na requisição: {e}")
                    if attempt < self.max_tries - 1:
                        wait_time = 2 * (attempt + 1)
                        print(f"-> Tentando novamente em {wait_time} segundos...")
                        time.sleep(wait_time)
                    else:
                        print("-> Número máximo de tentativas atingido.")
                        curr_url = None # Força a saída do loop principal 'while'
            
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
        Retorna todas as votações ocorridas nos anos especificados em forma de dicionario com os anos como chaves.
        '''
        endpoint = "votacoes"
        data = {}
        # a API aceita uma diferença de no máximo 3 meses entre as datas de inicio e fim
        intervalos = {
            0: ['01-01', '03-31'],
            1: ['04-01', '06-30'],
            2: ['07-01', '09-30'],
            3: ['10-01', '12-31']
        }
        # Apenas um ano por vez
        for ano in lista_anos:
            temp = []
            for i in range(4):
                parameters = {
                    'dataInicio': f'{ano}-{intervalos[i][0]}',
                    'dataFim': f'{ano}-{intervalos[i][1]}',
                    'itens': 100
                }
                temp.extend(self._get_all_data(endpoint, parameters))
            print(f"Total de itens coletados para o ano {ano}: {len(temp)}")
            data[ano] = temp
        return data
        
        

# url_vot = "https://dadosabertos.camara.leg.br/api/v2/votacoes"

# parameters = {
#     'dataInicio': "2019-07-01",
#     'dataFim': "2019-10-01",
#     'itens': 2
# }

# response = requests.get(url_vot, params=parameters, timeout=30)

# data = response.json()

# print(data['links'])

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
    
    # deputados = client.get_deputados(id_legislatura=56)
    
    # with open('deputados_raw.json', 'w') as f:
    #     json.dump(deputados, f, ensure_ascii=False, indent=4)
    
    anos = [2019, 2020, 2021, 2022]
        
    votacoes = client.get_votacoes(anos)
    for ano in anos:
        with open(f'votacoes_{ano}.json', 'w') as f:
            json.dump(votacoes[ano], f, ensure_ascii=False, indent=4)
    
    test = pd.DataFrame(votacoes)
    print(test)
    
