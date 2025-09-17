-- Cria ou substitui a View com a sua lógica de negócio
CREATE OR REPLACE VIEW vw_prata_chamados_sults AS
SELECT
    -- Seleciona as colunas que você precisa
    id,
    titulo,
    
    -- EXTRAI APENAS A DATA, IGNORANDO HORA E FUSO HORÁRIO
    DATE(resolvido) AS resolvido,
    
    solicitante_id,
    solicitante_nome,
    responsavel_id,
    responsavel_nome,
    departamento_id,
    departamento_nome,
    assunto_id,
    assunto_nome
FROM
    bronze_chamados_sults
WHERE
    -- (O restante da sua cláusula WHERE continua igual)
    (departamento_id = 1 AND situacao IN (1, 2))
    OR
    solicitante_id IN (58, 67, 70)
    OR
    EXISTS (
        SELECT 1
        FROM JSON_TABLE(
            REPLACE(REPLACE(REPLACE(apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
            '$[*]' 
            COLUMNS (
                dept_apoio VARCHAR(255) PATH '$.departamento.nome'
            )
        ) AS jt
        WHERE jt.dept_apoio = 'Marketing'
    );