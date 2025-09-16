INSERT INTO bronze_chamados_sults_apoio (
    id_apoio,
    id_chamado, 
    nome_apoio, 
    id_pessoa_apoio, 
    nome_departamento, 
    id_departamento, 
    pessoaUnidade,
    solicitante_id,
    solicitante_nome,
    responsavel_id,
    responsavel_nome
)
SELECT DISTINCT
    CONCAT(bcs.id, '-', jt.id_pessoa_apoio) AS id_apoio,
    bcs.id AS id_chamado,
    COALESCE(jt.nome_apoio, 'NÃO INFORMADO'),
    COALESCE(jt.id_pessoa_apoio, 0),
    COALESCE(jt.nome_departamento, 'NÃO INFORMADO'),
    COALESCE(jt.id_departamento, 0),
    COALESCE(jt.pessoaUnidade, FALSE),
    bcs.solicitante_id AS solicitante_id,
    bcs.solicitante_nome AS solicitante_nome,
    bcs.responsavel_id AS responsavel_id,
    bcs.responsavel_nome AS responsavel_nome
FROM 
    bronze_chamados_sults AS bcs,
    JSON_TABLE(
        REPLACE(REPLACE(REPLACE(bcs.apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
        '$[*]' 
        COLUMNS (
            nome_apoio        VARCHAR(255) PATH '$.pessapoiooa.nome',
            id_pessoa_apoio   INT          PATH '$.pessapoiooa.id',
            nome_departamento VARCHAR(255) PATH '$.departamento.nome',
            id_departamento   INT          PATH '$.departamento.id',
            pessoaUnidade     BOOLEAN      PATH '$.pessoaUnidade'
        )
    ) AS jt
WHERE bcs.apoio IS NOT NULL 
  AND bcs.apoio != '[]';


UPDATE 
    bronze_chamados_sults_apoio AS apoio
JOIN 
    bronze_chamados_sults AS chamados ON apoio.id_chamado = chamados.id
SET 
    apoio.situacao_chamado = CASE chamados.situacao
        WHEN 1 THEN 'Novo Chamado'
        WHEN 2 THEN 'Resolvido'
        WHEN 3 THEN 'Concluído'
        WHEN 4 THEN 'Em Andamento'
        WHEN 5 THEN 'Aguardando Solicitante'
        WHEN 6 THEN 'Aguardando Responsável'
        ELSE 'Outro' -- Para qualquer outro número que possa existir
    END;