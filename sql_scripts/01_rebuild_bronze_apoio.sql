-- Garante que a tabela seja sempre recriada do zero
DROP TABLE IF EXISTS bronze_chamados_sults_apoio;

-- Cria a estrutura vazia da tabela de apoio da bronze.
CREATE TABLE `bronze_chamados_sults_apoio` (
    `id_apoio` VARCHAR(255) PRIMARY KEY,
    `id_chamado` INT,
    `nome_apoio` VARCHAR(255),
    `id_pessoa_apoio` INT,
    `nome_departamento` VARCHAR(255),
    `id_departamento` INT,
    `pessoaUnidade` TINYINT(1),
    `solicitante_id` INT,
    `solicitante_nome` VARCHAR(255), -- Sugestão: Usar VARCHAR em vez de TEXT se houver limite de tamanho
    `responsavel_id` INT,
    `responsavel_nome` VARCHAR(255), -- Sugestão: Usar VARCHAR em vez de TEXT se houver limite de tamanho
    `situacao_chamado` VARCHAR(50)
);

-- Insere os dados já transformados em um único passo
INSERT INTO bronze_chamados_sults_apoio (
    id_apoio, id_chamado, nome_apoio, id_pessoa_apoio, nome_departamento, id_departamento, pessoaUnidade,
    solicitante_id, solicitante_nome, responsavel_id, responsavel_nome, situacao_chamado
)
SELECT DISTINCT
    CONCAT(bcs.id, '-', jt.id_pessoa_apoio) AS id_apoio,
    bcs.id AS id_chamado,
    COALESCE(jt.nome_apoio, 'NÃO INFORMADO') AS nome_apoio,
    COALESCE(jt.id_pessoa_apoio, 0) AS id_pessoa_apoio,
    COALESCE(jt.nome_departamento, 'NÃO INFORMADO') AS nome_departamento,
    COALESCE(jt.id_departamento, 0) AS id_departamento,
    COALESCE(jt.pessoaUnidade, FALSE) AS pessoaUnidade,
    bcs.solicitante_id,
    bcs.solicitante_nome,
    bcs.responsavel_id,
    bcs.responsavel_nome,
    CASE bcs.situacao
        WHEN 1 THEN 'Novo Chamado' WHEN 2 THEN 'Resolvido' WHEN 3 THEN 'Concluído'
        WHEN 4 THEN 'Em Andamento' WHEN 5 THEN 'Aguardando Solicitante' WHEN 6 THEN 'Aguardando Responsável'
        ELSE 'Outro'
    END AS situacao_chamado
FROM 
    bronze_chamados_sults AS bcs,
    JSON_TABLE(
        REPLACE(REPLACE(REPLACE(bcs.apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
        '$[*]' 
        COLUMNS (
            -- ATENÇÃO: Verifique se 'pessapoiooa' está correto. Pode ser 'pessoaApoio', por exemplo.
            nome_apoio        VARCHAR(255) PATH '$.pessapoiooa.nome',
            id_pessoa_apoio   INT          PATH '$.pessapoiooa.id',
            nome_departamento VARCHAR(255) PATH '$.departamento.nome',
            id_departamento   INT          PATH '$.departamento.id',
            pessoaUnidade     BOOLEAN      PATH '$.pessoaUnidade'
        )
    ) AS jt
WHERE bcs.apoio IS NOT NULL 
  AND bcs.apoio != '[]';