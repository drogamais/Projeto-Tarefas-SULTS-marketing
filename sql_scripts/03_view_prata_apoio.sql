CREATE OR REPLACE VIEW vw_prata_chamados_sults_apoio AS
SELECT
    id_apoio,
    id_chamado,

    CASE
        WHEN id_pessoa_apoio NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 'Outro'
        ELSE nome_apoio
    END AS nome_apoio,

    CASE
        WHEN id_pessoa_apoio NOT IN (SELECT id_sults FROM dim_responsaveis WHERE departamento_nome = "MARKETING") THEN 0
        ELSE id_pessoa_apoio
    END AS id_pessoa_apoio
    
FROM
    bronze_chamados_sults_apoio
WHERE
    situacao_chamado IN ('Resolvido', 'Concluído')
    AND id_departamento = 1;
