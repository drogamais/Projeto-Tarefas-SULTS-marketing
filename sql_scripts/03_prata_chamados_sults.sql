-- Apaga a tabela Prata antiga para garantir que os dados estão sempre atualizados
DROP TABLE IF EXISTS prata_chamados_sults;

-- Cria a nova tabela Prata com as colunas especificadas e a lógica de filtro
CREATE TABLE prata_chamados_sults AS
SELECT
    -- Seleciona apenas as colunas que você pediu
    id,
    titulo,
    resolvido,
    concluido,
    solicitante_id,
    solicitante_nome,
    responsavel_id,
    responsavel_nome, -- Corrigido
    departamento_id,
    departamento_nome,
    assunto_id,
    assunto_nome
FROM
    bronze_chamados_sults
WHERE
    -- CONDIÇÃO 1: O filtro que você já tinha
    (departamento_id = 1 AND situacao IN (1, 2) )

    OR
    
    solicitante_id IN (58,67,70)

    OR -- Adiciona as linhas que correspondem à nova condição

    -- CONDIÇÃO 2: Onde existe alguém do Marketing na coluna 'apoio'
    EXISTS (
        SELECT 1
        FROM JSON_TABLE(
            -- Trata a string para um JSON válido
            REPLACE(REPLACE(REPLACE(apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
            
            -- Usa o caminho que descobrimos que funciona
            '$[*]' 
            
            COLUMNS (
                dept_apoio VARCHAR(255) PATH '$.departamento.nome'
            )
        ) AS jt
        WHERE jt.dept_apoio = 'Marketing'
    );