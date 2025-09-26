# run_bronze.py

import sys
# Importa a função principal do seu script de sincronização.
# Garanta que o arquivo sync_chamados_sults.py esteja na mesma pasta.
from sync_chamados_sults import atualizar_camada_bronze

def main():
    """
    Função principal para executar a extração e carga na camada Bronze.
    """
    print("======================================================")
    print("== INICIANDO PROCESSO DE SINCRONIZAÇÃO DA CAMADA BRONZE ==")
    print("======================================================")
    
    try:
        # Chama a função que faz todo o trabalho de buscar na API e dar o UPSERT no banco.
        atualizar_camada_bronze()
        
        print("\n🎉 PROCESSO DA CAMADA BRONZE CONCLUÍDO COM SUCESSO! 🎉")

    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO NO PROCESSO DA CAMADA BRONZE.")
        print(f"   -> Detalhe do Erro: {e}")
        # Termina o script com um código de erro para indicar falha.
        sys.exit(1)

if __name__ == "__main__":
    main()