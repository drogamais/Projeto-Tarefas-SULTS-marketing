-- Cria ou substitui a View focada nos dados do solicitante
CREATE OR REPLACE VIEW vw_prata_chamados_sults_solicitante AS
-- ADICIONADO: Definição da CTE 'marketing_ids' que estava faltando
WITH marketing_ids AS (
    SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = 'MARKETING'
)
SELECT
    -- Seleciona e renomeia as colunas necessárias
    bcs.id_chamado,
    bcs.titulo,
    DATE(COALESCE(bcs.resolvido, bcs.aberto)) AS data,

    CASE
        WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 0
        ELSE bcs.solicitante_id
    END AS solicitante_id,
    
    CASE
        WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro'
        ELSE bcs.solicitante_nome
    END AS solicitante_nome,

        -- Busca o departamento da pessoa de apoio a partir da tabela dim_responsaveis
    COALESCE(dr_solicitante.departamento_id, 0) AS departamento_solicitante_id,
    COALESCE(dr_solicitante.departamento_nome, 'Não Informado') AS departamento_solicitante_nome,

    bcs.assunto_id,
    bcs.assunto_nome,

    bcs.situacao,
    -- Lógica para criar a coluna situacao_nome (CORRIGIDA)
    CASE bcs.situacao
        WHEN 1 THEN 'NOVO CHAMADO'
        WHEN 2 THEN 'CONCLUÍDO'
        WHEN 3 THEN 'RESOLVIDO'
        WHEN 4 THEN 'EM ANDAMENTO'
        WHEN 5 THEN 'AGUARDANDO SOLICITANTE'
        WHEN 6 THEN 'AGUARDANDO RESPONSÁVEL'
        ELSE 'SITUAÇÃO DESCONHECIDA' -- Adicionado como boa prática para casos inesperados
    END AS situacao_nome

FROM
    bronze_chamados_sults AS bcs

    LEFT JOIN
    dim_responsaveis AS dr_solicitante 
    ON bcs.solicitante_id = dr_solicitante.id_sults
WHERE
    -- CONDIÇÃO 1: Chamados do departamento 1 em situação 1 ou 2
    bcs.departamento_id = 1
