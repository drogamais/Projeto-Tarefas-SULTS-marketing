# run_etl_completo.py

import os
import mariadb
from config import DB_CONFIG
# Confirma que estamos importando a função principal correta
from sync_sults_marketing import atualizar_camada_bronze


def executar_scripts_da_pasta(path_da_pasta, cursor):
    """
    Lê e executa todos os arquivos .sql de uma pasta, em ordem alfabética/numérica.
    """
    print(f"\n--- EXECUTANDO SCRIPTS SQL DA PASTA: {path_da_pasta} ---")
    try:
        # Pega todos os arquivos da pasta que terminam com .sql e os ordena
        # A numeração (01_, 02_) garante a ordem correta de execução.
        scripts = sorted([f for f in os.listdir(path_da_pasta) if f.endswith('.sql')])
        
        if not scripts:
            print(f"AVISO: Nenhum script .sql encontrado em '{path_da_pasta}'.")
            return

        for script_file in scripts:
            filepath = os.path.join(path_da_pasta, script_file)
            print(f"-> Executando: {script_file}...")
            
            with open(filepath, 'r', encoding='utf-8') as sql_file:
                # O .split(';') é crucial para rodar múltiplos comandos dentro de um mesmo arquivo
                sql_commands = sql_file.read().split(';')
                for command in sql_commands:
                    # Garante que não executamos comandos vazios resultantes do split
                    if command.strip():
                        cursor.execute(command)
        
        print("✅ Todos os scripts SQL foram executados com sucesso.")

    except mariadb.Error as e:
        print(f"❌ ERRO ao executar o script '{script_file}'. A execução foi interrompida.")
        # Propaga o erro para que o bloco 'main' possa capturá-lo e parar o processo
        raise e
    except FileNotFoundError:
        print(f"❌ ERRO: A pasta de scripts '{path_da_pasta}' não foi encontrada.")
        raise
# ... (a função main continua a mesma) ...
def main():
    """
    Orquestra todo o processo de ETL na ordem correta, usando a lógica de UPSERT.
    """
    conn = None
    # Define a pasta onde seus scripts SQL modulares estão organizados
    SQL_SCRIPTS_FOLDER = "sql_scripts" 

    try:
        print("--- INICIANDO PROCESSO DE ETL COMPLETO ---")
        
        # --- PASSO 1: SINCRONIZAR A CAMADA BRONZE PRINCIPAL (UPSERT) ---
        # Esta função agora apenas insere/atualiza a tabela 'bronze_chamados_sults' via API.
        atualizar_camada_bronze()

        # --- PASSO 2: RECONSTRUIR COMPLETAMENTE AS CAMADAS DERIVADAS ---
        # Agora que a bronze principal está atualizada, reconstruímos tudo que depende dela.
        print("\n--- INICIANDO FASE DE TRANSFORMAÇÃO E CARGA (SQL) ---")
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Executa todos os scripts da pasta na ordem correta
        executar_scripts_da_pasta(SQL_SCRIPTS_FOLDER, cursor)
        
        # Confirma todas as transações SQL
        conn.commit()
        print("✅ Transformações SQL commitadas no banco de dados.")

        print("\n🎉 PROCESSO DE ETL COMPLETO CONCLUÍDO COM SUCESSO! 🎉")

    except Exception as e:
        # Pega qualquer erro, seja do Python (ex: API offline) ou do SQL (ex: sintaxe errada)
        print(f"\n❌ ERRO CRÍTICO NO PROCESSO DE ORQUESTRAÇÃO. A execução foi interrompida.")
        print(f"  -> Detalhe do Erro: {e}")
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")
if __name__ == "__main__":
    main()