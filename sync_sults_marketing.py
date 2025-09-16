import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import mariadb  # ADICIONADO: Usaremos a biblioteca mariadb diretamente
from config import DB_CONFIG # Importa a configuração

# --- CONFIGURAÇÃO GERAL ---
API_TOKEN = "O2Ryb2dhbWFpczsxNzQ0ODAzNDc1NjIx"
BASE_URL = "https://api.sults.com.br/api/v1"

headers = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json;charset=UTF-8",
}

def buscar_todos_chamados(filtros=None):
    """
    Busca todos os chamados da API da SULTS.
    (Esta função permanece sem alterações)
    """
    if filtros is None:
        filtros = {}
    endpoint_chamados = "/chamado/ticket"
    url_chamados = f"{BASE_URL}{endpoint_chamados}"
    todos_chamados = []
    pagina_atual = 0
    limit_por_pagina = 100
    print(f"Iniciando a busca de chamados no endpoint: '{endpoint_chamados}'...")
    print(f"Filtros aplicados: {filtros if filtros else 'Nenhum (buscando todos os dados)'}")
    with tqdm(desc="Buscando páginas de chamados") as pbar:
        while True:
            params = filtros.copy()
            params['start'] = pagina_atual
            params['limit'] = limit_por_pagina
            try:
                response = requests.get(url_chamados, headers=headers, params=params)
                response.raise_for_status()
                resposta_json = response.json()
                dados_da_pagina = resposta_json.get("data", [])
                if not dados_da_pagina:
                    pbar.set_description("Busca concluída!")
                    break
                todos_chamados.extend(dados_da_pagina)
                pagina_atual += 1
                pbar.update(1)
                pbar.set_postfix_str(f"{len(todos_chamados)} chamados encontrados")
            except requests.exceptions.RequestException as e:
                print(f"\nErro ao buscar a página {pagina_atual}: {e}")
                break
    if not todos_chamados:
        print("Nenhum chamado encontrado com os filtros especificados.")
        return pd.DataFrame()
    print(f"\nTotal de {len(todos_chamados)} chamados encontrados.")
    return pd.json_normalize(todos_chamados, sep='_')

# --- FUNÇÃO DE CARGA MODIFICADA ---
def salvar_na_camada_bronze(df, nome_tabela, db_config):
    """
    Salva o DataFrame em uma tabela do MariaDB usando a biblioteca 'mariadb'.
    Esta função irá apagar e recriar a tabela a cada execução (modo 'replace').
    """
    conn = None
    try:
        # Conecta ao banco de dados usando os parâmetros do config
        print("\nConectando ao banco de dados MariaDB...")
        conn = mariadb.connect(**db_config)
        cursor = conn.cursor()
        print("✅ Conexão estabelecida com sucesso!")

        print(f"Preparando para carregar {len(df)} registros na tabela '{nome_tabela}'...")
        
        # 1. Apaga a tabela antiga, se ela existir
        cursor.execute(f"DROP TABLE IF EXISTS {nome_tabela}")
        
        # 2. Cria a nova tabela (Pandas ajuda a gerar o SQL de criação)
        # Usamos a função do Pandas para gerar o CREATE TABLE, que é complexo de fazer manualmente
        create_table_sql = pd.io.sql.get_schema(df, nome_tabela)
        # Pequeno ajuste para garantir compatibilidade com MariaDB
        create_table_sql = create_table_sql.replace('"', '`') 
        cursor.execute(create_table_sql)

        # 3. Insere os dados de forma eficiente
        # Converte o DataFrame para uma lista de tuplas (formato que o executemany espera)
        dados_para_inserir = [tuple(x) for x in df.to_numpy()]
        
        # Cria a string de inserção, ex: INSERT INTO tabela VALUES (?, ?, ?, ...)
        sql_insert = f"INSERT INTO {nome_tabela} VALUES ({','.join(['?'] * len(df.columns))})"
        
        cursor.executemany(sql_insert, dados_para_inserir)
        
        # 4. Confirma a transação
        conn.commit()
        print(f"✅ Carga de {cursor.rowcount} registros para a camada Bronze concluída!")

    except mariadb.Error as e:
        print(f"❌ Erro ao interagir com o MariaDB: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    
    filtros_de_busca = {}
    
    # 1. EXTRAIR: Puxa os dados da API
    df_chamados_brutos = buscar_todos_chamados(filtros=filtros_de_busca)
    
    if not df_chamados_brutos.empty:
        # Limpeza para evitar problemas na inserção
        df_chamados_brutos = df_chamados_brutos.astype(object).where(pd.notnull(df_chamados_brutos), None)
        
        # 2. CARREGAR: Salva os dados brutos na tabela bronze
        salvar_na_camada_bronze(df_chamados_brutos, "bronze_chamados_sults", DB_CONFIG)