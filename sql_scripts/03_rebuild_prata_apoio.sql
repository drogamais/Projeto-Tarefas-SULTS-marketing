-- Apaga a tabela Prata antiga
DROP TABLE IF EXISTS prata_chamados_sults_apoio;

-- Cria a nova tabela Prata sem constraints
CREATE TABLE prata_chamados_sults_apoio AS
SELECT
    id_apoio,
    id_chamado, 
    nome_apoio, 
    id_pessoa_apoio, 
    nome_departamento, 
    id_departamento,
    solicitante_id,
    solicitante_nome,
    responsavel_id,
    responsavel_nome
FROM 
    bronze_chamados_sults_apoio
WHERE 
    situacao_chamado IN ('Resolvido', 'Concluído')
    AND id_departamento = 1;

-- Adiciona a chave primária
ALTER TABLE prata_chamados_sults_apoio
ADD PRIMARY KEY (id_apoio);

-- Adiciona a foreign key
ALTER TABLE prata_chamados_sults_apoio
ADD CONSTRAINT fk_chamado
FOREIGN KEY (id_chamado) REFERENCES bronze_chamados_sults(id);
