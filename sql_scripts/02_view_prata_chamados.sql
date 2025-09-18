-- Cria ou substitui a View com a lógica de IDs como 0 para 'OUTRO'
CREATE OR REPLACE VIEW vw_prata_chamados_sults AS
SELECT
    -- Colunas que não mudam
    id,
    titulo,
    -- O DATE() foi removido no seu último script, estou mantendo assim. Se precisar, adicione-o de volta: DATE(resolvido) AS resolvido
    DATE(resolvido) AS data,
    
    -- --- LÓGICA PARA SOLICITANTE ---
    -- Se o ID não estiver na lista, vira 0, senão mantém o original.
    CASE
        WHEN solicitante_id NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    
    -- Se o ID (original) não estiver na lista, o NOME vira 'OUTRO'.
    CASE
        WHEN solicitante_id NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 'Outro'
        ELSE solicitante_nome
    END AS solicitante_nome,
    
    -- --- LÓGICA PARA RESPONSÁVEL ---
    -- Se o ID não estiver na lista, vira 0, senão mantém o original.
    CASE
        WHEN responsavel_id NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 0
        ELSE responsavel_id
    END AS responsavel_id,

    -- Se o ID (original) não estiver na lista, o NOME vira 'OUTRO'.
    CASE
        WHEN responsavel_id NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 'Outro'
        ELSE responsavel_nome
    END AS responsavel_nome,
    
    -- Colunas restantes
    departamento_id,
    departamento_nome,
    assunto_id,
    assunto_nome
FROM
    bronze_chamados_sults
WHERE
    -- (Sua cláusula WHERE foi mantida exatamente como você enviou)
    (departamento_id = 1 AND situacao IN (2, 3))
    AND
    (
        solicitante_id IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING")
        OR
        EXISTS (
            SELECT 1
            FROM JSON_TABLE(
                REPLACE(REPLACE(REPLACE(apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
                '$[*]' 
                COLUMNS (
                    dept_apoio VARCHAR(255) PATH '$.departamento.nome'
                )
            ) AS jt
            WHERE jt.dept_apoio = 'Marketing'
        )
    );