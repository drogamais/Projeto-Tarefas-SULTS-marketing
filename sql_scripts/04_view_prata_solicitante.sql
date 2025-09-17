-- Cria ou substitui a View focada nos dados do solicitante
CREATE OR REPLACE VIEW vw_prata_chamados_sults_solicitante AS
SELECT
    -- Seleciona e renomeia as colunas necessárias
    id AS id_chamado,
    solicitante_nome,
    solicitante_id
FROM
    bronze_chamados_sults
WHERE
    -- MANTÉM EXATAMENTE A MESMA LÓGICA DE FILTRAGEM DA VIEW ANTERIOR
    -- para garantir que ambas as views representem o mesmo conjunto de chamados.
    
    -- CONDIÇÃO 1: Chamados do departamento 1 em situação 1 ou 2
    (departamento_id = 1 AND situacao IN (1, 2))

    OR
    
    -- CONDIÇÃO 2: Chamados de solicitantes específicos
    solicitante_id IN (58, 67, 70)

    OR

    -- CONDIÇÃO 3: Onde existe alguém do Marketing na coluna 'apoio'
    EXISTS (
        SELECT 1
        FROM JSON_TABLE(
            -- Trata a string para um JSON válido
            REPLACE(REPLACE(REPLACE(apoio, "'", '"'), ': True', ': true'), ': False', ': false'),
            
            -- Caminho para extrair os objetos do array JSON
            '$[*]' 
            
            -- Define a coluna a ser extraída de cada objeto
            COLUMNS (
                dept_apoio VARCHAR(255) PATH '$.departamento.nome'
            )
        ) AS jt
        WHERE jt.dept_apoio = 'Marketing'
    );