@echo off
echo [PASSO 1 de 2] Executando script SQL para atualizar a camada prata...
mariadb -h seu_host -u seu_usuario -pSuaSenha seu_banco < cria_prata.sql

echo.
echo [PASSO 2 de 2] Executando script Python para atualizar a camada bronze...
python .\sync_sults_marketing.py

echo.
echo Processo Concluido!
pause