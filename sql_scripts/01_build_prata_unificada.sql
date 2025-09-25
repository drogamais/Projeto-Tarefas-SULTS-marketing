-- Garante que a tabela seja sempre recriada do zero para refletir as atualizações da camada bronze.
DROP TABLE IF EXISTS prata_chamados_unificada;

-- Cria a nova tabela prata unificada, consolidando toda a lógica.
CREATE TABLE prata_chamados_marketing AS

-- CTE para definir a lista de IDs do Marketing.
WITH marketing_ids AS (
    SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = 'MARKETING'
),

-- ETAPA 1: Expande o JSON para ter uma linha por pessoa de apoio.
bronze_com_apoio AS (
    SELECT
        bcs.*, -- Pega todas as colunas originais da tabela bronze
        jt.nome_apoio AS nome_pessoa_apoio, -- Renomeia para evitar ambiguidade
        jt.id_pessoa_apoio
    FROM
        bronze_chamados_sults AS bcs
    LEFT JOIN
        JSON_TABLE(
            REPLACE(REPLACE(REPLACE(bcs.apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
            '$[*]'
            COLUMNS (
                nome_apoio        VARCHAR(255) PATH '$.pessapoiooa.nome',
                id_pessoa_apoio   INT          PATH '$.pessapoiooa.id'
            )
        ) AS jt ON 1=1
),

-- ETAPA 2: Filtra de forma definitiva qualquer registro que contenha os IDs indesejados.
chamados_filtrados AS (
    SELECT *
    FROM bronze_com_apoio
    WHERE
        solicitante_id NOT IN (1, 41)
    AND responsavel_id NOT IN (1, 41)
    AND COALESCE(id_pessoa_apoio, 0) NOT IN (1, 41)
)

-- ETAPA 3: Com os dados já limpos, aplica a lógica de negócio (inclusão e transformação).
SELECT
    -- 1. CHAVE PRIMÁRIA ARTIFICIAL (ID DO FATO)
    CONCAT(bcs.id_chamado, '-', COALESCE(bcs.id_pessoa_apoio, 0)) AS id_fato_chamado,

    -- 2. DADOS DO CHAMADO (DIMENSÕES DEGENERADAS)
    bcs.id_chamado,
    bcs.titulo,
    CAST(LEFT(COALESCE(bcs.resolvido, bcs.aberto, '1900-01-01'), 10) AS DATE) AS data_referencia,
    bcs.assunto_id,
    bcs.assunto_nome,
    bcs.situacao,
    CASE bcs.situacao
        WHEN 1 THEN 'NOVO CHAMADO' WHEN 2 THEN 'CONCLUÍDO' WHEN 3 THEN 'RESOLVIDO'
        WHEN 4 THEN 'EM ANDAMENTO' WHEN 5 THEN 'AGUARDANDO SOLICITANTE' WHEN 6 THEN 'AGUARDANDO RESPONSÁVEL'
        ELSE 'SITUAÇÃO DESCONHECIDA'
    END AS situacao_nome,

    -- 3. DADOS DO SOLICITANTE
    CASE WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 0 ELSE bcs.solicitante_id END AS solicitante_id,
    CASE WHEN bcs.solicitante_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro' ELSE bcs.solicitante_nome END AS solicitante_nome,
    COALESCE(dr_solicitante.departamento_nome, 'Não Informado') AS departamento_solicitante_nome,

    -- 4. DADOS DO RESPONSÁVEL
    CASE WHEN bcs.responsavel_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 0 ELSE bcs.responsavel_id END AS responsavel_id,
    CASE WHEN bcs.responsavel_id NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro' ELSE bcs.responsavel_nome END AS responsavel_nome,
    COALESCE(dr_responsavel.departamento_nome, 'Não Informado') AS departamento_responsavel_nome,

    -- 5. DADOS DA PESSOA DE APOIO
    CASE
        WHEN bcs.id_pessoa_apoio IS NULL THEN NULL -- Se não existe apoio, ID é NULO
        WHEN bcs.id_pessoa_apoio NOT IN (SELECT id_sults FROM marketing_ids) THEN 0 -- Se é "Outro", ID é 0
        ELSE bcs.id_pessoa_apoio -- Caso contrário, usa o ID real
    END AS id_pessoa_apoio,
    
    CASE
        WHEN bcs.id_pessoa_apoio IS NULL THEN NULL -- Se não existe apoio, Nome é NULO
        WHEN bcs.id_pessoa_apoio NOT IN (SELECT id_sults FROM marketing_ids) THEN 'Outro' -- Se é "Outro", Nome é 'Outro'
        ELSE COALESCE(bcs.nome_pessoa_apoio, 'Nome não informado')
    END AS nome_apoio,
    
    CASE
        WHEN bcs.id_pessoa_apoio IS NULL THEN NULL -- Se não existe apoio, Depto é NULO
        ELSE COALESCE(dr_apoio.departamento_nome, 'Departamento não informado')
    END AS departamento_apoio_nome

FROM
    chamados_filtrados AS bcs -- A FONTE AGORA É A TABELA JÁ FILTRADA

LEFT JOIN
    dim_responsaveis AS dr_solicitante ON bcs.solicitante_id = dr_solicitante.id_sults
LEFT JOIN
    dim_responsaveis AS dr_apoio ON bcs.id_pessoa_apoio = dr_apoio.id_sults
LEFT JOIN
    dim_responsaveis AS dr_responsavel ON bcs.responsavel_id = dr_responsavel.id_sults

WHERE
    -- Apenas a lógica de INCLUSÃO é necessária aqui. A exclusão já foi feita.
    bcs.departamento_id = 1
    OR bcs.solicitante_id IN (SELECT id_sults FROM marketing_ids)
    OR bcs.id_pessoa_apoio IN (SELECT id_sults FROM marketing_ids);