-- 1. Garante que a tabela antiga seja removida antes de começar.
DROP TABLE IF EXISTS prata_chamados_sults;

-- 2. Cria a estrutura da tabela vazia, definindo a PRIMARY KEY.
CREATE TABLE prata_chamados_sults (
    id_fato_chamado                 VARCHAR(255) PRIMARY KEY,
    id_chamado                      INT,
    titulo                          TEXT,
    data_referencia                 DATE,
    assunto_id                      INT,
    assunto_nome                    VARCHAR(255),
    situacao                        INT,
    situacao_nome                   VARCHAR(255),
    solicitante_id                  INT,
    solicitante_nome                VARCHAR(255),
    departamento_solicitante_nome   VARCHAR(255),
    responsavel_id                  INT,
    responsavel_nome                VARCHAR(255),
    departamento_responsavel_nome   VARCHAR(255),
    id_pessoa_apoio                 INT,
    nome_apoio                      VARCHAR(255),
    departamento_apoio_nome         VARCHAR(255),
    tipo_origem                     VARCHAR(100)
);

-- 3. Insere os dados na tabela recém-criada usando a lógica de transformação.
INSERT INTO prata_chamados_sults
-- ETAPA 1: Expande o JSON para ter uma linha por pessoa de apoio.
WITH bronze_com_apoio AS (
    SELECT DISTINCT
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

-- ETAPA 3: Com os dados já limpos, seleciona e transforma as colunas para a inserção.
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
    bcs.solicitante_id,
    -- MODIFICADO: Usa o nome_oficial da dim_responsaveis com fallback.
    COALESCE(dr_solicitante.nome_oficial, bcs.solicitante_nome) AS solicitante_nome,
    COALESCE(dr_solicitante.departamento_nome, 'Não Informado') AS departamento_solicitante_nome,

    -- 4. DADOS DO RESPONSÁVEL
    bcs.responsavel_id,
    COALESCE(dr_responsavel.nome_oficial, bcs.responsavel_nome) AS responsavel_nome,
    COALESCE(dr_responsavel.departamento_nome, 'Não Informado') AS departamento_responsavel_nome,

    -- 5. DADOS DA PESSOA DE APOIO
    bcs.id_pessoa_apoio,
    -- MODIFICADO: Usa o nome_oficial da dim_responsaveis com fallback, dentro da lógica que trata nulos.
    CASE 
        WHEN bcs.id_pessoa_apoio IS NULL THEN NULL 
        ELSE COALESCE(dr_apoio.nome_oficial, bcs.nome_pessoa_apoio, 'Nome não informado') 
    END AS nome_apoio,
    
    CASE
        WHEN bcs.id_pessoa_apoio IS NULL THEN NULL
        ELSE COALESCE(dr_apoio.departamento_nome, 'Departamento não informado')
    END AS departamento_apoio_nome,
    
    -- 6. DADOS DE ORIGEM
    bcs.tipo_origem

FROM
    chamados_filtrados AS bcs -- A FONTE AGORA É A TABELA JÁ FILTRADA

LEFT JOIN
    dim_responsaveis AS dr_solicitante ON bcs.solicitante_id = dr_solicitante.id_sults
LEFT JOIN
    dim_responsaveis AS dr_apoio ON bcs.id_pessoa_apoio = dr_apoio.id_sults
LEFT JOIN
    dim_responsaveis AS dr_responsavel ON bcs.responsavel_id = dr_responsavel.id_sults;