import pandas as pd
import requests
from tqdm import tqdm
import os

votings_file = 'data/processed/votacoes.csv'

votings = pd.read_csv(votings_file, encoding='latin-1')
votings = votings.astype({'temVotacao': 'boolean'})

votes_file = 'data/processed/votos.csv'

url_base = "https://dadosabertos.camara.leg.br/api/v2/votacoes"

# Prepara o arquivo de votos
final_columns = ['id_votacao', 'voto', 'deputado_id', 'deputado_nome', 'partido', 'uf']
if not os.path.exists(votes_file):
    print(f"Arquivo '{votes_file}' não encontrado. Criando arquivo com cabeçalho.")
    pd.DataFrame(columns=final_columns).to_csv(votes_file, index=False, encoding='utf-8')
else:
    print(f"Anexando dados ao arquivo existente: {votes_file}")
    
indices_to_check = votings[votings['temVotacao'].isnull()].index

for index in indices_to_check:
    desc = votings.loc[index, 'descricao']
    if not isinstance(desc, str) or 'total' not in desc.lower():
        votings.loc[index, 'temVotacao'] = False
        votings.loc[index, 'totalVotos'] = 0



# Votações que ainda não checamos se há votos e possivelmente têm
ids_remaining = votings[votings['temVotacao'].isnull()].index
print(f"Total de votações a verificar: {len(ids_remaining)}")

count_store = 0
for index in tqdm(ids_remaining):
    try:
        # Pega o ID da votação a partir do índice
        id_votacao = votings.loc[index, 'id']
        
        response = requests.get(f'{url_base}/{id_votacao}/votos', timeout=10)
        response.raise_for_status()  # Lança erro se não completou a chamada
        
        data = response.json()
        
        if bool(data.get('dados')):
            votings.loc[index, 'temVotacao'] = True
            votings.loc[index, 'totalVotos'] = len(data['dados'])
            
            # Usa json_normalize para achatar o JSON em um DataFrame
            df_new_votes = pd.json_normalize(data['dados'])
            
            # Adiciona ID da votação
            df_new_votes['id_votacao'] = id_votacao
            
            # Renomeia colunas
            df_new_votes = df_new_votes.rename(columns={
                'tipoVoto': 'voto',
                'deputado_.id': 'deputado_id',
                'deputado_.nome': 'deputado_nome',
                'deputado_.siglaPartido': 'partido',
                'deputado_.siglaUf': 'uf'
            })
            
            
            df_new_votes = df_new_votes.reindex(columns=final_columns)
            
            # Adiciona os novos votos no arquivo
            df_new_votes.to_csv(
                votes_file,
                mode='a',
                header=False,
                index=False,
                encoding='utf-8'
            )

        else:
            votings.loc[index, 'temVotacao'] = False
            
    except requests.exceptions.RequestException as e:
        tqdm.write(f"Erro de conexão para o ID {id_votacao}: {e}.")
        continue
    
    
    # Salva de 100 em 100 no arquivo de votações
    count_store += 1
    if count_store % 100 == 0:
        tqdm.write(f"\nSalvando progresso em '{votings_file}'...")
        votings.to_csv(votings_file, index=False, encoding='latin-1')
        tqdm.write("Progresso salvo.")

# Fim
print("\nSalvando o estado final do arquivo de votações.")
votings.to_csv(votings_file, index=False, encoding='latin-1')

#Salvar um arquivo com apenas as votações que têm votos registrados
real_votings = votings[votings['temVotacao'] == True]
real_votings.to_csv('data/processed/votacoes_reais.csv', index=False, encoding='latin-1')

print("Script finalizado com sucesso!")