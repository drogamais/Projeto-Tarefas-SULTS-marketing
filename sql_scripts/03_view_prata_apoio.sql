CREATE OR REPLACE VIEW vw_prata_chamados_sults_apoio AS
-- Otimização com CTE para definir a lista de IDs do Marketing apenas uma vez
WITH marketing_ids AS (
    SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = 'MARKETING'
)

SELECT
    bca.id_apoio,
    bca.id_chamado,

    -- Lógica para o nome do apoio, agora usando a CTE
    CASE
        WHEN bca.id_pessoa_apoio NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro'
        ELSE bca.nome_apoio
    END AS nome_apoio,

    -- Lógica para o ID do apoio, agora usando a CTE
    CASE
        WHEN bca.id_pessoa_apoio NOT IN (SELECT id_sults FROM marketing_ids) THEN 0
        ELSE bca.id_pessoa_apoio
    END AS id_pessoa_apoio,

    -- Busca o departamento da pessoa de apoio a partir da tabela dim_responsaveis
    COALESCE(dr_apoio.departamento_id, 0) AS departamento_apoio_id,
    COALESCE(dr_apoio.departamento_nome, 'Não Informado') AS departamento_apoio_nome,

    bca.data
    
FROM
    bronze_chamados_sults_apoio AS bca

-- Juntamos a tabela de apoio com a de responsáveis PARA ENCONTRAR O DEPARTAMENTO DA PESSOA DE APOIO
LEFT JOIN
    dim_responsaveis AS dr_apoio 
    ON bca.id_pessoa_apoio = dr_apoio.id_sults

WHERE
    bca.situacao_chamado IN ('Resolvido', 'Concluído')
    AND bca.id_departamento = 1;