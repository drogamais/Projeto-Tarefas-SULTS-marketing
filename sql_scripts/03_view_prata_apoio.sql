CREATE OR REPLACE VIEW vw_prata_chamados_sults_apoio AS
SELECT
    id_apoio,
    id_chamado,
    nome_apoio,
    id_pessoa_apoio
FROM
    bronze_chamados_sults_apoio
WHERE
    situacao_chamado IN ('Resolvido', 'Concluído')
    AND id_departamento = 1;
