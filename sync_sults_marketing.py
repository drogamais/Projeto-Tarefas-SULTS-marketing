import requests
import pandas as pd
from tqdm import tqdm
import mariadb
from config import DB_CONFIG

# --- FUNÇÕES DE BUSCA E TRANSFORMAÇÃO (sem alterações) ---
# ... (as funções buscar_todos_chamados e transformar_dataframe_bronze continuam exatamente as mesmas) ...
API_TOKEN = "O2Ryb2dhbWFpczsxNzQ0ODAzNDc1NjIx"
BASE_URL = "https://api.sults.com.br/api/v1"

headers = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json;charset=UTF-8",
}

def buscar_todos_chamados(filtros=None):
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

def transformar_dataframe_bronze(df):

    if 'id' in df.columns:
        print("Renomeando coluna 'id' para 'id_chamado'...")
        df.rename(columns={'id': 'id_chamado'}, inplace=True)

    print("Iniciando transformação de tipos de dados no DataFrame...")
    colunas_datas = [
        'aberto', 'resolvido', 'concluido', 'resolverPlanejado',
        'resolverEstipulado', 'primeiraInteracao', 'ultimaAlteracao'
    ]
    for col in colunas_datas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    colunas_numericas = [
        'id', 'tipo', 'situacao', 'solicitante_id', 'responsavel_id',
        'unidade_id', 'departamento_id', 'assunto_id',
        'countInteracaoPublico', 'countInteracaoInterno'
    ]
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    print("✅ Transformação de tipos concluída.")
    return df

def criar_tabela_se_nao_existir(nome_tabela, db_config):
    """Verifica se a tabela existe e a cria se necessário."""
    conn = None
    try:
        conn = mariadb.connect(**db_config)
        cursor = conn.cursor()
        
        # Query para criar a tabela (mesma estrutura do SQL acima)
        # Usar CREATE TABLE IF NOT EXISTS é seguro e só executa se a tabela não existir.
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {nome_tabela} (
            id_chamado BIGINT NOT NULL PRIMARY KEY,
            titulo TEXT,
            aberto DATETIME,
            resolvido DATETIME,
            concluido DATETIME,
            resolverPlanejado DATETIME,
            resolverEstipulado DATETIME,
            primeiraInteracao DATETIME,
            ultimaAlteracao DATETIME,
            tipo BIGINT,
            situacao BIGINT,
            solicitante_id BIGINT,
            solicitante_nome VARCHAR(255),
            responsavel_id BIGINT,
            responsavel_nome VARCHAR(255),
            unidade_id BIGINT,
            unidade_nome VARCHAR(255),
            departamento_id BIGINT,
            departamento_nome VARCHAR(255),
            assunto_id BIGINT,
            assunto_nome VARCHAR(255),
            countInteracaoPublico BIGINT,
            countInteracaoInterno BIGINT,
            apoio JSON,
            etiqueta TEXT,
            avaliacaoNota TEXT
        );
        """
        
        print(f"Verificando e, se necessário, criando a tabela '{nome_tabela}'...")
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Tabela pronta para uso.")
        
    except mariadb.Error as e:
        print(f"❌ Erro ao verificar/criar a tabela: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def upsert_camada_bronze(df, nome_tabela, db_config):
    """
    Realiza um 'UPSERT' no MariaDB.
    Insere novas linhas e atualiza as existentes com base na chave primária 'id'.
    Assume que a tabela já existe.
    """
    conn = None
    if df.empty:
        print("DataFrame vazio, nenhum dado para carregar.")
        return

    try:
        print("\nConectando ao banco de dados MariaDB...")
        conn = mariadb.connect(**db_config)
        cursor = conn.cursor()
        print("✅ Conexão estabelecida com sucesso!")
        print(f"Preparando para fazer o UPSERT de {len(df)} registros na tabela '{nome_tabela}'...")

        # 1. Prepara a query de UPSERT
        colunas = [f"`{col}`" for col in df.columns]
        colunas_str = ", ".join(colunas)
        placeholders_str = ", ".join(['?'] * len(df.columns))

        # Cria a parte "UPDATE" da query
        update_clause = ", ".join([f"{col} = VALUES({col})" for col in colunas if col.lower() != '`id_chamado`'])
        
        sql_upsert = (
            f"INSERT INTO `{nome_tabela}` ({colunas_str}) "
            f"VALUES ({placeholders_str}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
        
        # 2. Prepara os dados
        df_para_db = df.astype(object).where(pd.notnull(df), None)
        dados_para_inserir = [tuple(x) for x in df_para_db.to_numpy()]

        # 3. Executa a query
        cursor.executemany(sql_upsert, dados_para_inserir)
        conn.commit()
        
        # A propriedade .rowcount em um UPSERT retorna:
        # 1 para cada inserção nova.
        # 2 para cada atualização.
        # 0 se nada mudou.
        print(f"✅ Carga Upsert concluída! Status de linhas afetadas: {cursor.rowcount}")

    except mariadb.Error as e:
        # Erro comum: a tabela não existe. Damos uma dica para o usuário.
        if "Table" in str(e) and "doesn't exist" in str(e):
             print(f"❌ ERRO: A tabela '{nome_tabela}' parece não existir.")
             print("   -> DICA: Para a primeira execução, use o script anterior (com DROP/CREATE) para criar a tabela com o schema correto.")
        else:
            print(f"❌ Erro ao interagir com o MariaDB: {e}")
        raise e
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

#--- FUNÇÃO PRINCIPAL PARA O ORQUESTRADOR ---
def atualizar_camada_bronze():
    """
    Encapsula todo o processo de E+T+L da fonte (API) para a camada Bronze.
    """
    print("--- INICIANDO SUBPROCESSO: ATUALIZAÇÃO DA CAMADA BRONZE ---")
    
    # Adicione a chamada da nova função AQUI
    criar_tabela_se_nao_existir("bronze_chamados_sults", DB_CONFIG)
    
    # 1. EXTRAIR
    df_chamados_brutos = buscar_todos_chamados()
    
    if not df_chamados_brutos.empty:
        # 2. TRANSFORMAR
        df_chamados_tratados = transformar_dataframe_bronze(df_chamados_brutos)
        
        # 3. CARREGAR (usando a função de upsert)
        upsert_camada_bronze(df_chamados_tratados, "bronze_chamados_sults", DB_CONFIG)
    print("--- SUBPROCESSO BRONZE FINALIZADO ---")

# --- BLOCO DE EXECUÇÃO INDEPENDENTE (PARA TESTES) ---
if __name__ == "__main__":
    atualizar_camada_bronze()