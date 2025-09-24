-- Cria ou substitui a View com a nova lógica e otimizações
CREATE OR REPLACE VIEW vw_prata_chamados_sults AS
-- Otimização com CTE para definir a lista de IDs do Marketing apenas uma vez
WITH marketing_ids AS (
    SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = 'MARKETING'
)
-- Fim da CTE

SELECT
    -- Colunas da tabela de chamados
    bcs.id_chamado,
    bcs.titulo,
    
    DATE(COALESCE(bcs.resolvido, bcs.aberto)) AS data,
    
    -- Lógica para Responsável (já estava correta)
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

WHERE
    bcs.departamento_id = 1 