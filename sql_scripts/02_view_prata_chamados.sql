-- Cria ou substitui a View com a nova lógica e otimizações
CREATE OR REPLACE VIEW vw_prata_chamados_sults AS
-- Passo 1: Otimização com CTE para definir a lista de IDs do Marketing apenas uma vez
WITH marketing_ids AS (
    SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = 'MARKETING'
)
-- Fim da CTE

SELECT
    -- Colunas da tabela de chamados
    bcs.id,
    bcs.titulo,
    DATE(bcs.resolvido) AS data,
    
    -- --- LÓGICA PARA SOLICITANTE ---
    CASE
        WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 0
        ELSE bcs.solicitante_id
    END AS solicitante_id,
    
    CASE
        WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro'
        ELSE bcs.solicitante_nome
    END AS solicitante_nome,
    
    -- <<< INÍCIO DA NOVA LÓGICA >>>
    -- Busca o departamento do solicitante a partir da tabela dim_responsaveis que foi unida (JOIN)
    COALESCE(dr_solicitante.departamento_id, 0) AS departamento_solicitante_id,
    COALESCE(dr_solicitante.departamento_nome, 'Não Informado') AS departamento_solicitante_nome,
    -- <<< FIM DA NOVA LÓGICA >>>

    -- --- LÓGICA PARA RESPONSÁVEL ---
    CASE
        WHEN bcs.responsavel_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 0
        ELSE bcs.responsavel_id
    END AS responsavel_id,

    CASE
        WHEN bcs.responsavel_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro'
        ELSE bcs.responsavel_nome
    END AS responsavel_nome,
    
    -- Colunas restantes da tabela de chamados
    bcs.departamento_id,
    bcs.departamento_nome,
    bcs.assunto_id,
    bcs.assunto_nome

FROM
    bronze_chamados_sults AS bcs

-- Passo 2: Juntamos a tabela de chamados com a de responsáveis PARA ENCONTRAR O DEPARTAMENTO DO SOLICITANTE
LEFT JOIN
    dim_responsaveis AS dr_solicitante 
    ON bcs.solicitante_id = dr_solicitante.id_sults

WHERE
    (bcs.departamento_id = 1 AND bcs.situacao IN (2, 3))
    AND
    (
        bcs.solicitante_id IN (SELECT id_sults FROM marketing_ids)
        OR
        EXISTS (
            SELECT 1
            FROM JSON_TABLE(
                REPLACE(REPLACE(REPLACE(bcs.apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
                '$[*]' 
                COLUMNS (
                    dept_apoio VARCHAR(255) PATH '$.departamento.nome'
                )
            ) AS jt
            WHERE jt.dept_apoio = 'Marketing'
        )
    );