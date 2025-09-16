# run_etl_completo.py

import mariadb
from config import DB_CONFIG
# Importe a função que você criou no outro arquivo
from sync_sults_marketing import buscar_e_salvar_bronze

def executar_script_sql(filepath, cursor):
    """Lê e executa um arquivo SQL inteiro."""
    print(f"Executando script SQL: {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as sql_file:
        # Usamos split(';') para rodar múltiplos comandos no arquivo, se houver
        sql_commands = sql_file.read().split(';')
        for command in sql_commands:
            if command.strip(): # Garante que não executamos comandos vazios
                cursor.execute(command)
    print("✅ Script concluído.")

def main():
    """
    Orquestra todo o processo de ETL na ordem correta.
    """
    conn = None
    try:
        print("--- INICIANDO PROCESSO DE ETL COMPLETO ---")
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # --- PASSO 1: ATUALIZAR A CAMADA PRATA (ou simplesmente dropar) ---
        # Isso remove as dependências (chaves estrangeiras) da tabela bronze.
        # Se seu script SQL já tem o DROP, ótimo. Se não, adicione.
        executar_script_sql('scripts/01_cria_camada_prata.sql', cursor)
        conn.commit()

        # --- PASSO 2: ATUALIZAR A CAMADA BRONZE ---
        # Agora que não há mais 'filhos', o script da bronze pode rodar sem erros.
        buscar_e_salvar_bronze() # Esta seria sua lógica do script atual, refatorada em uma função

        print("--- PROCESSO DE ETL CONCLUÍDO COM SUCESSO ---")

    except mariadb.Error as e:
        print(f"❌ Erro no processo de orquestração: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Para rodar este script, você precisaria ajustar seu outro arquivo
    # para que a lógica principal esteja dentro de uma função importável,
    # como a 'buscar_e_salvar_bronze()' que usei de exemplo.
    main()